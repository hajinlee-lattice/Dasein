package com.latticeengines.datacloud.collection.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.ecs.ECSService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.collection.service.CollectionDBService;
import com.latticeengines.datacloud.collection.service.CollectionRequestService;
import com.latticeengines.datacloud.collection.service.CollectionWorkerService;
import com.latticeengines.datacloud.collection.service.RawCollectionRequestService;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.S3PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateCollectionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;

@Service("collectionDBService")
public class CollectionDBServiceImpl implements CollectionDBService {

    private static final Logger log = LoggerFactory.getLogger(CollectionDBServiceImpl.class);
    private static final int LATENCY_GAP_MS = 3000;
    private static final int BUCKET_CACHE_LIMIT = 1024;

    @Inject
    private RawCollectionRequestService rawCollectionRequestService;

    @Inject
    private CollectionWorkerService collectionWorkerService;

    @Inject
    private CollectionRequestService collectionRequestService;

    @Inject
    private S3Service s3Service;

    @Inject
    private ECSService ecsService;

    @Inject
    private VendorConfigMgr vendorConfigMgr;

    @Inject
    private VendorConfigService vendorConfigService;

    @Inject
    private TransformationProxy transformationProxy;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Value("${datacloud.collection.s3bucket.prefix}")
    private String s3BucketPrefix;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${datacloud.collection.ecr.image.name}")
    private String ecrImageName;

    @Value("${datacloud.collection.ecs.cluster.name}")
    private String ecsClusterName;

    @Value("${datacloud.collection.ecs.task.def.name}")
    private String ecsTaskDefName;

    @Value("${datacloud.collection.ecs.task.cpu}")
    private String ecsTaskCpu;

    @Value("${datacloud.collection.ecs.task.memory}")
    private String ecsTaskMemory;

    @Value("${datacloud.collection.ecs.task.subnets}")
    private String ecsTaskSubnets;

    @Value("${datacloud.collection.bw.timestamp}")
    private String bwTimestampColumn;

    @Value("${datacloud.collection.ingestion.partion.period}")
    private int ingestionPartionPeriod;

    @Value("${datacloud.collection.alexa.timestamp}")
    private String alexaTimestampColumn;

    @Value("${datacloud.collection.semrush.timestamp}")
    private String semrushTimestampColumn;

    @Value("${datacloud.ingestion.column.id}")
    private String ingestionIdCol;

    @Value("${datacloud.ingestion.column.pid}")
    private String ingestionPidCol;

    @Value("${datacloud.collection.orbintelligencev2.timestamp}")
    private String orbIntelligenceV2TimestampColumn;

    /*
    @Value("${datacloud.collection.consolidation.exec.period}")
    private int consolidationExecPeriod;

    @Value("${datacloud.collection.consolidation.exec.weekday}")
    private int consolidationExecWeekDay;*/

    private long prevCollectMillis = 0;
    private long prevCleanupMillis = 0;
    private int prevCollectTasks;
    private long prevIngestionMillis = 0;

    public boolean addNewDomains(List<String> domains, String vendor, String reqId) {

        return rawCollectionRequestService.addNewDomains(domains, vendor, reqId);

    }

    public void addNewDomains(List<String> domains, String reqId) {

        Iterable<String> vendors = VendorConfig.EFFECTIVE_VENDOR_SET;

        vendors.forEach(vendor -> addNewDomains(domains, vendor, reqId));

    }

    private int transferRawRequests(boolean deleteFilteredReqs) {

        List<RawCollectionRequest> rawReqs = rawCollectionRequestService.getNonTransferred();

        BitSet filter = collectionRequestService.addNonTransferred(rawReqs);

        rawCollectionRequestService.updateTransferredStatus(rawReqs, filter, deleteFilteredReqs);

        if (rawReqs.size() > 0) {

            log.info("TRANSFER_RAW_COLLECTION_REQ=" + rawReqs.size() + "," + filter.cardinality());
            log.info("CREATE_COLLECTION_REQ=" + (rawReqs.size() - filter.cardinality()));

        }

        return rawReqs.size() - filter.cardinality();

    }

    private File generateCSV(List<CollectionRequest> readyReqs) throws Exception {

        File tempFile = File.createTempFile("temp-", ".csv");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {

            writer.write("Domain");
            writer.newLine();

            for (CollectionRequest req : readyReqs) {

                writer.write(req.getDomain());
                writer.newLine();

            }

        }

        return tempFile;

    }

    public int spawnCollectionWorker() throws Exception {

        int spawnedTasks = 0;

        int maxRetries = vendorConfigService.getDefMaxRetries();
        int collectingBatch = vendorConfigService.getDefCollectionBatch();
        for (String vendor : VendorConfig.EFFECTIVE_VENDOR_SET) {

            //fixme: is the earliest time of collecting reqs enough? may be ready reqs should also be considered?
            //get earliest active request time
            Timestamp earliestReqTime = collectionRequestService.getEarliestTime(vendor, CollectionRequest.STATUS_COLLECTING);

            //get stopped worker since that time
            List<CollectionWorker> stoppedWorkers = collectionWorkerService.getWorkerStopped(vendor, earliestReqTime);

            //modify req status to READY | FAILED based on retry times
            int modified = collectionRequestService.handlePending(vendor, maxRetries, stoppedWorkers);
            if (modified > 0) {

                log.info("find " + stoppedWorkers.size() + " " + vendor + " workers recently stopped");
                log.info(modified + " pending " + vendor + " collection requests reset to ready");

            }

            //check rate limit
            int activeTasks = collectionWorkerService.getActiveWorkerCount(vendor);
            int taskLimit = vendorConfigService.getMaxActiveTasks(vendor);
            if (activeTasks >= taskLimit) {

                continue;

            }

            //get ready reqs
            List<CollectionRequest> readyReqs = collectionRequestService.getReady(vendor, collectingBatch);
            if (CollectionUtils.isEmpty(readyReqs)) {

                continue;

            }

            //generate input csv
            File tempCsv = generateCSV(readyReqs);

            //generate worker id
            String workerId = UUID.randomUUID().toString().toUpperCase();

            //upload to s3
            String prefix = s3BucketPrefix + workerId + "/input/domains.csv";
            s3Service.uploadLocalFile(s3Bucket, prefix, tempCsv, true);
            FileUtils.deleteQuietly(tempCsv);

            //spawn worker in aws, '-v vendor -w worker_id'
            String cmdLine = "-v " + vendor + " -w " + workerId;
            String taskArn = ecsService.spawECSTask(
                    ecsClusterName,
                    ecsTaskDefName,
                    "python",
                    cmdLine,
                    ecsTaskSubnets);

            //create worker record in
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            CollectionWorker worker = new CollectionWorker();
            worker.setWorkerId(workerId);
            worker.setVendor(vendor);
            worker.setStatus(CollectionWorker.STATUS_NEW);
            worker.setSpawnTime(ts);
            worker.setTaskArn(taskArn);

            collectionWorkerService.getEntityMgr().create(worker);

            //update request status
            collectionRequestService.beginCollecting(readyReqs, worker);

            log.info("BEG_COLLECTING_REQ=" + vendor + "," + readyReqs.size());

            readyReqs.clear();

            ++spawnedTasks;

        }

        if (spawnedTasks > 0) {

            log.info("SPAWN_COLLECTION_WORKER=" + spawnedTasks);

        }

        return spawnedTasks;

    }

    private Map<String, Task> getTasksByWorkers(List<CollectionWorker> workers) {

        List<String> taskArns = workers.stream().map(CollectionWorker::getTaskArn).collect(Collectors.toList());

        List<Task> tasks = ecsService.getTasks(ecsClusterName, taskArns);
        HashMap<String, Task> arn2tasks = new HashMap<>(tasks.size() * 2);

        for (Task task : tasks) {
            arn2tasks.put(task.getTaskArn(), task);
        }

        return arn2tasks;

    }

    private long getDomainFromCsvEx(String vendor, File csvFile, Set<String> domains, Set<String> errDomains) throws Exception {

        long ret = 0;
        String domainField = vendorConfigService.getDomainField(vendor);
        String domainCheckField = vendorConfigService.getDomainCheckField(vendor);

        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile)))) {
            try (CSVParser parser = new CSVParser(reader, format)) {

                Map<String, Integer> colMap = parser.getHeaderMap();
                int domainIdx = colMap.getOrDefault(domainField, -1);
                int domainChkIdx = colMap.getOrDefault(domainCheckField, -1);

                if (domainIdx == -1 || domainChkIdx == -1) {

                    return ret;

                }

                for (CSVRecord rec : parser) {

                    ++ret;
                    String domain = rec.get(domainIdx);

                    if (!rec.get(domainChkIdx).equals("")) {

                        domains.add(domain);

                    } else {

                        errDomains.add(domain);

                    }


                }

                return ret;

            }

        }

    }

    private List<File> downloadWorkerOutput(CollectionWorker worker) throws Exception {

        String workerId = worker.getWorkerId();

        //list file in s3 output path
        String prefix = s3BucketPrefix + workerId + "/output/";
        List<S3ObjectSummary> itemDescs = s3Service.listObjects(s3Bucket, prefix);
        if (itemDescs.size() < 1) {

            return null;

        }

        //download content
        List<File> tmpFiles = new ArrayList<>(itemDescs.size());
        for (S3ObjectSummary itemDesc : itemDescs) {

            if (itemDesc.getSize() == 0) {

                continue;

            }

            File tmpFile = File.createTempFile("temp", ".csv");
            tmpFile.deleteOnExit();
            tmpFiles.add(tmpFile);

            s3Service.downloadS3File(itemDesc, tmpFile);

        }

        return tmpFiles;

    }

    private boolean handleFinishedTask(CollectionWorker worker) throws Exception {

        String workerId = worker.getWorkerId();

        //download worker output
        List<File> tmpFiles = downloadWorkerOutput(worker);
        if (CollectionUtils.isEmpty(tmpFiles)) {

            return false;

        }

        String vendor = worker.getVendor();

        /*
        //copy to hdfs
        String hdfsDir = hdfsPathBuilder.constructCollectorWorkerDir(vendor, workerId).toString();
        log.info("about to upload collected data to hdfs: " + hdfsDir);
        HdfsUtils.mkdir(yarnConfiguration, hdfsDir);

        for (File tmpFile : tmpFiles) {

            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, tmpFile.getPath(), hdfsDir);

        }*/

        //parse csv
        Set<String> domains = new HashSet<>();
        Set<String> errDomains = new HashSet<>();
        long recordsCollected = 0;

        for (File tmpFile : tmpFiles) {

            recordsCollected += getDomainFromCsvEx(vendor, tmpFile, domains, errDomains);

        }

        worker.setRecordsCollected(recordsCollected);

        log.info("END_COLLECTING_REQ=" + vendor + "," + domains.size() + "," + recordsCollected + "," + errDomains.size());

        //consumeFinished reqs
        collectionRequestService.consumeFinished(workerId, domains);
        domains.clear();

        //clean tmp files
        for (File tmpFile : tmpFiles) {

            FileUtils.deleteQuietly(tmpFile);

        }

        return true;

    }

    /*
    Check CollectionWorker table. From TaskARN find the status of the worker by AWS SDK.

    If some worker is started but not terminated, update CollectionWorker to Status=RUNNING
    If some worker is finished successfully:
        move the output csv to the vendor's raw ingestion folder
        scan the csv, update CollectionRequest table for each collected domain
            Due to the behavior of spider, all collected domains are treated as delivered (just the content may be empty)
        update status of the worker in CollectionWorker table
    If some worker is finished with error
        update status of the worker in CollectionWorker table
     */
    private int updateCollectingStatus() throws Exception {

        int stoppedTasks = 0;

        //get the 'active' workers
        List<String> statusList = Arrays.asList(//
                CollectionWorker.STATUS_NEW, //
                CollectionWorker.STATUS_RUNNING, //
                CollectionWorker.STATUS_FINISHED //
        );
        List<CollectionWorker> activeWorkers = collectionWorkerService.getWorkerByStatus(statusList);
        if (CollectionUtils.isEmpty(activeWorkers)) {

            return 0;

        }

        //get corresponding aws ecs tasks
        Map<String, Task> activeTasks = getTasksByWorkers(activeWorkers);
        activeTasks = MapUtils.emptyIfNull(activeTasks);

        //handling active worker/task
        int failedTasks = 0;
        for (CollectionWorker worker : activeWorkers) {

            Task task = activeTasks.get(worker.getTaskArn());

            if (task != null) {

                if (CollectionWorker.STATUS_NEW.equals(worker.getStatus()) &&
                        (task.getLastStatus().equals("RUNNING") || task.getLastStatus().equals("STOPPED"))) {

                    log.info("task " + worker.getWorkerId() + " starts running");

                    worker.setStatus(CollectionWorker.STATUS_RUNNING);
                    collectionWorkerService.getEntityMgr().update(worker);

                }

                if (!task.getLastStatus().equals("STOPPED")) {

                    continue;

                }

            } else if (!CollectionWorker.STATUS_FINISHED.equals(worker.getStatus())) {

                log.info("task " + worker.getWorkerId() + " loses traces, mark it as finished now...");

                worker.setStatus(CollectionWorker.STATUS_FINISHED);
            }

            //transfer state to finished
            if (CollectionWorker.STATUS_RUNNING.equals(worker.getStatus())) {

                log.info("task " + worker.getWorkerId() + " finished running");

                worker.setStatus(CollectionWorker.STATUS_FINISHED);
                collectionWorkerService.getEntityMgr().update(worker);

            }

            //consuming output
            //download csv
            //Files.createTempDirectory()
            //no csv file: status => fail
            //copy csv file to hdfs
            if (CollectionWorker.STATUS_FINISHED.equals(worker.getStatus())) {

                log.info("task " + worker.getWorkerId() + " finished, starts consuming its output");

                boolean succ = handleFinishedTask(worker);

                //status => consumed/failed
                worker.setTerminationTime(task != null ? new Timestamp(task.getStoppedAt().getTime()) : new Timestamp
                        (System.currentTimeMillis()));
                worker.setStatus(succ ? CollectionWorker.STATUS_CONSUMED : CollectionWorker.STATUS_FAILED);

                collectionWorkerService.getEntityMgr().update(worker);

                ++stoppedTasks;
                if (!succ) {

                    ++failedTasks;

                }

                log.info("task " + worker.getWorkerId() + (succ ? " consumed" : " failed"));

            }

        }

        if (stoppedTasks > 0) {

            log.info("COLLECTION_WORKER_STOPPED=" + stoppedTasks + "," + failedTasks);

        }

        return stoppedTasks;

    }

    public int getActiveTaskCount() {

        List<String> statusList = Arrays.asList(//
                CollectionWorker.STATUS_NEW, //
                CollectionWorker.STATUS_RUNNING, //
                CollectionWorker.STATUS_FINISHED //
        );
        List<CollectionWorker> activeWorkers = collectionWorkerService.getWorkerByStatus(statusList);

        return CollectionUtils.size(activeWorkers);

    }

    public void cleanup() {

        rawCollectionRequestService.cleanup();

        long ts = System.currentTimeMillis();
        for (String vendor : VendorConfig.EFFECTIVE_VENDOR_SET) {

            long period = vendorConfigService.getCollectingFreq(vendor); //in seconds

            long tsBefore = ts - period * 1000;

            collectionRequestService.cleanupRequestHandled(vendor, new Timestamp(tsBefore));
        }
    }

    public boolean collect() {
        if (prevCollectMillis == 0) {

            log.info("datacloud collection job starts...");

        }
        long currentMillis = System.currentTimeMillis();

        boolean finished = true;
        int activeTasks = 0;
        try {

            if (prevCleanupMillis == 0 || currentMillis - prevCleanupMillis > 86400 * 1000) {

                prevCleanupMillis = currentMillis;

                log.info("begin cleaning up unused raw reqs and collection reqs...");
                cleanup();
                log.info("cleaning up done.");

            }

            activeTasks = getActiveTaskCount();
            if (prevCollectMillis == 0 ||
                    prevCollectTasks != activeTasks ||
                    currentMillis - prevCollectMillis >= 3600 * 1000) {

                prevCollectMillis = currentMillis;
                log.info("There're " + activeTasks + " tasks");

            }
            prevCollectTasks = activeTasks;

            int reqs = transferRawRequests(true);
            if (reqs > 0) {
                log.info(reqs + " requests transferred from raw requests to collection requests");
                finished = false;
            }
            Thread.sleep(LATENCY_GAP_MS);

            int newTasks = spawnCollectionWorker();
            activeTasks += newTasks;
            if (newTasks > 0) {
                log.info(newTasks + " new collection workers spawned, active tasks => " + activeTasks);
                finished = false;
            }
            Thread.sleep(LATENCY_GAP_MS);

            int stoppedTasks = updateCollectingStatus();
            activeTasks -= stoppedTasks;
            if (stoppedTasks > 0) {
                log.info(stoppedTasks + " tasks stopped, active tasks => " + activeTasks);
                finished = false;
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return activeTasks == 0 && finished;

    }

    private String getTimestampColumn(String vendor) throws Exception {

        switch (vendor) {
            case VendorConfig.VENDOR_BUILTWITH:
                return bwTimestampColumn;
            case VendorConfig.VENDOR_ALEXA:
                return alexaTimestampColumn;
            case VendorConfig.VENDOR_SEMRUSH:
                return semrushTimestampColumn;
            case VendorConfig.VENDOR_ORBI_V2:
                return orbIntelligenceV2TimestampColumn;
            default:
                throw new Exception("not implemented");
        }

    }

    private Pair<Long, Long> getTimestampRange(List<File> inputFiles, String vendor) throws Exception {

        long maxTs = Long.MIN_VALUE;
        long minTs = Long.MAX_VALUE;

        String tsCol = getTimestampColumn(vendor);

        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);

        for (File file : inputFiles) {

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                try (CSVParser parser = new CSVParser(reader, format)) {

                    Map<String, Integer> colMap = parser.getHeaderMap();
                    int tsIdx = colMap.getOrDefault(tsCol, -1);
                    if (tsIdx == -1) {

                        continue;

                    }

                    for (CSVRecord csvRec : parser) {

                        String tsText = csvRec.get(tsIdx);
                        try {

                            long tsVal = Long.parseLong(tsText);

                            if (tsVal > maxTs) {

                                maxTs = tsVal;

                            }

                            if (tsVal < minTs) {

                                minTs = tsVal;

                            }

                        } catch (Exception e) {
                            log.error(tsText + " is not a valid timestamp value");
                        }

                    }

                }

            }

        }

        if (minTs == Long.MAX_VALUE || maxTs == Long.MIN_VALUE) {

            log.error("timestamp range wrong: [" + minTs + ", " + maxTs + "]");
            throw new Exception("wrong timestamp range");

        }

        return Pair.of(minTs, maxTs);
    }

    private Schema getSchema(String vendor) throws Exception {
        switch (vendor) {
            case VendorConfig.VENDOR_BUILTWITH:
                return new Schema.Parser().parse(AvroUtils.buildSchema("builtwith.avsc"));
            case VendorConfig.VENDOR_ALEXA:
                return new Schema.Parser().parse(AvroUtils.buildSchema("alexa.avsc"));
            case VendorConfig.VENDOR_SEMRUSH:
                return new Schema.Parser().parse(AvroUtils.buildSchema("semrush.avsc"));
            case VendorConfig.VENDOR_ORBI_V2:
                return new Schema.Parser().parse(AvroUtils.buildSchema("orbIntelligence.avsc"));
            default:
                throw new Exception("not implemented");
        }
    }

    /*
    private List<File> createBucketFiles(long minTs, int periodInMS, int bucketCount) throws Exception {

        List<File> bucketFiles = new ArrayList<>();

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String path = Files.createTempDirectory("bucket").toString();
        Date date = new Date();

        for (int i = 0; i < bucketCount; ++i) {

            date.setTime(minTs + i * periodInMS);

            File of = new File(path, dateFormat.format(date) + "_UTC.avro");
            of.deleteOnExit();

            bucketFiles.add(of);

        }

        return bucketFiles;

    }

    private List<File> doBucketing(List<File> inputFiles, String vendor) throws Exception {

        //get timestamp, adjust min to whole period
        Pair<Long, Long> tsPair = getTimestampRange(inputFiles, vendor);
        long minTs = tsPair.getKey();
        long maxTs = tsPair.getValue();

        int periodInMS = ingestionPartionPeriod * 1000;
        int dayInMS = 86400 * 1000;
        int weekInMS = dayInMS * 7;
        if (periodInMS == weekInMS) {

            minTs -= minTs % dayInMS;//adjust to day boundary
            long weekDay = (minTs % weekInMS / dayInMS + 4) % 7;//week day
            minTs -= weekDay * dayInMS;

        } else {

            minTs -= minTs % periodInMS;

        }

        //create bucket file/buffers
        int bucketCount = 1 + (int) ((maxTs - minTs) / periodInMS);

        List<File> bucketFiles = createBucketFiles(minTs, periodInMS, bucketCount);

        boolean[] bucketCreated = new boolean[bucketCount];

        List<List<GenericRecord>> bucketBuffers = new ArrayList<>();
        for (int i = 0; i < bucketCount; ++i) {

            bucketBuffers.add(new ArrayList<>());

        }

        //processing files
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);
        Schema schema = getSchema(vendor);
        List<Schema.Field> fields = schema.getFields();
        String tsColName = getTimestampColumn(vendor);
        String domainCheckField = vendorConfigService.getDomainCheckField(vendor);

        for (File file : inputFiles) {

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                try (CSVParser parser = new CSVParser(reader, format)) {

                    Map<String, Integer> csvColName2Idx = parser.getHeaderMap();
                    int domainChkCsvCol = csvColName2Idx.getOrDefault(domainCheckField, -1);
                    int columnCount = csvColName2Idx.size();

                    //calculate col mapping
                    int[] csvIdx2Avro = new int[columnCount];
                    Schema.Field[] avroIdx2Fields = new Schema.Field[columnCount];
                    if (fields.size() != columnCount + 2) {//avro has two extra id column

                        throw new Exception("avro column count != csv column count");

                    }

                    for (Schema.Field field: fields) {

                        int csvIdx = csvColName2Idx.getOrDefault(field.name(), -1);
                        if (csvIdx == -1) {

                            continue;

                        }

                        csvIdx2Avro[csvIdx] = field.pos();
                        avroIdx2Fields[csvIdx] = field;

                    }
                    int tsAvroCol = csvIdx2Avro[csvColName2Idx.get(tsColName)];
                    Schema.Field avroIdField = schema.getField(ingestionIdCol);
                    Schema.Field avroPidField = schema.getField(ingestionPidCol);

                    //iterate csv, generate avro
                    for (CSVRecord csvRec : parser) {

                        if (domainChkCsvCol != -1 && csvRec.get(domainChkCsvCol).equals("")) {

                            //bypass dummy line
                            continue;

                        }

                        //create avro record, add it to buffer
                        GenericRecord rec = new GenericData.Record(schema);
                        for (int i = 0; i < columnCount; ++i) {

                            rec.put(csvIdx2Avro[i], AvroUtils.checkTypeAndConvertEx(avroIdx2Fields[i].name(), //
                                    csvRec.get(i), avroIdx2Fields[i]));

                        }
                        rec.put(avroIdField.pos(), UUID.randomUUID().toString());
                        rec.put(avroPidField.pos(), null);

                        int bucketIdx = (int) (((Long) rec.get(tsAvroCol) - minTs) / periodInMS);
                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        buf.add(rec);

                        //flush bucket buffer when it's full
                        if (buf.size() == BUCKET_CACHE_LIMIT) {

                            if (!bucketCreated[bucketIdx]) {

                                AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketIdx).getPath(), true);
                                bucketCreated[bucketIdx] = true;

                            } else {

                                AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketIdx).getPath(), true);

                            }

                            buf.clear();

                        }

                    }

                    //flush non-empty buffers
                    for (int bucketIdx = 0; bucketIdx < bucketCount; ++bucketIdx) {

                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        if (buf.size() == 0) {

                            continue;

                        }

                        if (!bucketCreated[bucketIdx]) {

                            AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketIdx).getPath(), true);
                            bucketCreated[bucketIdx] = true;

                        } else {

                            AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketIdx).getPath(), true);

                        }

                        buf.clear();

                    }

                }

            }

        }

        return bucketFiles;

    }

    private void ingestConsumedWorker(CollectionWorker worker) throws Exception {

        try {

            String vendor = worker.getVendor();
            log.info("ingesting " + vendor + ", worker_id = " + worker.getWorkerId());

            List<File> tmpFiles = downloadWorkerOutput(worker);
            if (CollectionUtils.isEmpty(tmpFiles)) {

                log.error(worker.getVendor() + " worker " + worker.getWorkerId() //
                        + "\'s output dir on s3 does not contain any files");
                return;

            }

            //bucketing
            List<File> bucketFiles = doBucketing(tmpFiles, vendor);
            List<File> tmpBucketFiles = new ArrayList<>(bucketFiles.size());
            if (CollectionUtils.isNotEmpty(bucketFiles)) {

                Path ingestLocation = S3PathBuilder.constructIngestionDir(vendor + "_RAW");
                String ingestDir = ingestLocation.toString();

                for (File bucketFile : bucketFiles) {

                    if (!bucketFile.exists()) {

                        log.warn("no data gathered for bucket file: " + bucketFile.getPath());
                        continue;

                    }

                    String remoteFilePath = ingestLocation.append(bucketFile.getName()).toString();

                    if (s3Service.objectExist(s3Bucket, remoteFilePath)) {

                        File tmpFile = File.createTempFile("temp", ".avro");
                        tmpFile.deleteOnExit();
                        tmpBucketFiles.add(tmpFile);

                        s3Service.downloadS3File(s3Service.listObjects(s3Bucket, remoteFilePath).get(0), tmpFile);

                        if (bucketFile.length() >= tmpFile.length()) {

                            log.info("appending to local file: " + bucketFile);
                            AvroUtils.appendToLocalFile(tmpFile.getPath(), bucketFile.getPath(), true);

                            log.info("uploading local file: " + bucketFile);
                            s3Service.uploadLocalFile(s3Bucket, remoteFilePath, bucketFile, true);

                        } else {

                            log.info("appending to local file: " + tmpFile);
                            AvroUtils.appendToLocalFile(bucketFile.getPath(), tmpFile.getPath(), true);

                            log.info("uploading local file: " + tmpFile);
                            s3Service.uploadLocalFile(s3Bucket, remoteFilePath, tmpFile, true);
                        }

                    } else {

                        log.info("uploading local file: " + bucketFile);
                        s3Service.uploadLocalFile(s3Bucket, remoteFilePath, bucketFile, true);

                    }

                    log.info("uploaded s3 file in bucket " + s3Bucket + ": " + remoteFilePath);

                }

            }

            //clean local file
            for (File tmpFile : tmpFiles) {

                FileUtils.deleteQuietly(tmpFile);

            }

            if (CollectionUtils.isNotEmpty(bucketFiles)) {

                for (File bucketFile : bucketFiles) {

                    FileUtils.deleteQuietly(bucketFile);

                }

                for (File tmpBucketFile: tmpBucketFiles) {

                    FileUtils.deleteQuietly(tmpBucketFile);

                }
            }

            //update worker status
            worker.setStatus(CollectionWorker.STATUS_INGESTED);
            collectionWorkerService.getEntityMgr().update(worker);

        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }*/

    private void createBucketFiles(long minTs, int periodInMS, int bucketCount, Map<Long, File> buckets) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String path = Files.createTempDirectory("bucket").toString();
        Date date = new Date();

        long ts = minTs;
        for (int i = 0; i < bucketCount; ++i, ts += periodInMS) {

            if (buckets.containsKey(ts)) {

                continue;

            }

            date.setTime(ts);

            File of = new File(path, dateFormat.format(date) + "_UTC.avro");
            of.deleteOnExit();

            buckets.put(ts, of);

        }

    }

    private void doBucketing(List<File> inputFiles, String vendor, Map<Long, File> bucketFiles) throws Exception {

        //get timestamp, adjust min to whole period
        Pair<Long, Long> tsPair = getTimestampRange(inputFiles, vendor);
        long minTs = tsPair.getKey();
        long maxTs = tsPair.getValue();

        int periodInMS = ingestionPartionPeriod * 1000;
        int dayInMS = 86400 * 1000;
        int weekInMS = dayInMS * 7;
        if (periodInMS == weekInMS) {

            minTs -= minTs % dayInMS;//adjust to day boundary
            long weekDay = (minTs % weekInMS / dayInMS + 4) % 7;//week day
            minTs -= weekDay * dayInMS;

        } else {

            minTs -= minTs % periodInMS;

        }

        //create bucket file/buffers
        int bucketCount = 1 + (int) ((maxTs - minTs) / periodInMS);

        createBucketFiles(minTs, periodInMS, bucketCount, bucketFiles);

        boolean[] bucketCreated = new boolean[bucketCount];

        List<List<GenericRecord>> bucketBuffers = new ArrayList<>();
        for (int i = 0; i < bucketCount; ++i) {

            bucketBuffers.add(new ArrayList<>());

        }

        //processing files
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);
        Schema schema = getSchema(vendor);
        List<Schema.Field> fields = schema.getFields();
        String tsColName = getTimestampColumn(vendor);
        String domainCheckField = vendorConfigService.getDomainCheckField(vendor);

        for (File file : inputFiles) {

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                try (CSVParser parser = new CSVParser(reader, format)) {

                    Map<String, Integer> csvColName2Idx = parser.getHeaderMap();
                    int domainChkCsvCol = csvColName2Idx.getOrDefault(domainCheckField, -1);
                    int columnCount = csvColName2Idx.size();

                    //calculate col mapping
                    int[] csvIdx2Avro = new int[columnCount];
                    Schema.Field[] avroIdx2Fields = new Schema.Field[columnCount];
                    if (fields.size() != columnCount + 2) {//avro has two extra id column

                        throw new Exception("avro column count != csv column count");

                    }

                    for (Schema.Field field: fields) {

                        int csvIdx = csvColName2Idx.getOrDefault(field.name(), -1);
                        if (csvIdx == -1) {

                            continue;

                        }

                        csvIdx2Avro[csvIdx] = field.pos();
                        avroIdx2Fields[csvIdx] = field;

                    }
                    int tsAvroCol = csvIdx2Avro[csvColName2Idx.get(tsColName)];
                    Schema.Field avroIdField = schema.getField(ingestionIdCol);
                    Schema.Field avroPidField = schema.getField(ingestionPidCol);

                    //iterate csv, generate avro
                    for (CSVRecord csvRec : parser) {

                        if (domainChkCsvCol != -1 && csvRec.get(domainChkCsvCol).equals("")) {

                            //bypass dummy line
                            continue;

                        }

                        //create avro record, add it to buffer
                        GenericRecord rec = new GenericData.Record(schema);
                        for (int i = 0; i < columnCount; ++i) {

                            rec.put(csvIdx2Avro[i], AvroUtils.checkTypeAndConvertEx(avroIdx2Fields[i].name(), //
                                    csvRec.get(i), avroIdx2Fields[i]));

                        }
                        rec.put(avroIdField.pos(), UUID.randomUUID().toString());
                        rec.put(avroPidField.pos(), null);

                        int bucketIdx = (int) (((Long) rec.get(tsAvroCol) - minTs) / periodInMS);
                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        buf.add(rec);

                        //flush bucket buffer when it's full
                        if (buf.size() == BUCKET_CACHE_LIMIT) {

                            long bucketTs = minTs + periodInMS * bucketIdx;
                            if (!bucketCreated[bucketIdx]) {

                                AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketTs).getPath(), true);
                                bucketCreated[bucketIdx] = true;

                            } else {

                                AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketTs).getPath(), true);

                            }

                            buf.clear();

                        }

                    }

                    //flush non-empty buffers
                    for (int bucketIdx = 0; bucketIdx < bucketCount; ++bucketIdx) {

                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        if (buf.size() == 0) {

                            continue;

                        }

                        long bucketTs = minTs + periodInMS * bucketIdx;
                        if (!bucketCreated[bucketIdx]) {

                            AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketTs).getPath(), true);
                            bucketCreated[bucketIdx] = true;

                        } else {

                            AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketTs).getPath(), true);

                        }

                        buf.clear();

                    }

                }

            }

        }

    }

    private boolean ingestConsumedWorker(CollectionWorker worker, Map<String, Map<Long, File>> bucketGroups) {

        boolean ret = false;
        try {

            String vendor = worker.getVendor();
            log.info("ingesting " + vendor + ", worker_id = " + worker.getWorkerId());

            if (!bucketGroups.containsKey(vendor)) {

                bucketGroups.put(vendor, new HashMap<>());

            }
            Map<Long, File> bucketFiles = bucketGroups.get(vendor);

            List<File> tmpFiles = downloadWorkerOutput(worker);
            if (CollectionUtils.isEmpty(tmpFiles)) {

                log.warn(worker.getVendor() + " worker " + worker.getWorkerId() //
                        + "\'s output dir on s3 does not contain any files");
                return true;

            }

            //bucketing
            doBucketing(tmpFiles, vendor, bucketFiles);

            //clean local file
            for (File tmpFile : tmpFiles) {

                FileUtils.deleteQuietly(tmpFile);

            }

            ret = true;
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return ret;
    }

    private void uploadIngestedBucket(Map<String, Map<Long, File>> bucketGroups) throws Exception {

        if (bucketGroups.isEmpty()) {

            return;

        }

        //success file
        File tagFile = File.createTempFile("temp", "tag");
        tagFile.createNewFile();
        tagFile.deleteOnExit();

        //loop all vendor
        for (Map.Entry<String, Map<Long, File>> entry : bucketGroups.entrySet()) {

            String vendor = entry.getKey();
            Map<Long, File> bucketFiles = entry.getValue();

            if (bucketFiles.isEmpty()) {

                log.info("0 ingested files to upload for " + vendor);
                continue;

            }

            log.info("uploading ingested file for " + vendor);

            List<File> tmpBucketFiles = new ArrayList<>(bucketFiles.size());
            Path ingestLocation = S3PathBuilder.constructIngestionDir(vendor + "_RAW");

            //loop all bucket files
            for (File bucketFile : bucketFiles.values()) {

                if (!bucketFile.exists()) {

                    log.warn("no data gathered for bucket file: " + bucketFile.getName());
                    continue;

                }

                String fileName = bucketFile.getName();
                String fileTs = fileName.substring(0, fileName.length() - ".avro".length());
                Path dirLocation = ingestLocation.append(fileTs);
                String remoteFilePath = dirLocation.append(bucketFile.getName()).toString();

                if (s3Service.objectExist(s3Bucket, remoteFilePath)) {

                    File tmpFile = File.createTempFile("temp", ".avro");
                    tmpFile.deleteOnExit();
                    tmpBucketFiles.add(tmpFile);

                    s3Service.downloadS3File(s3Service.listObjects(s3Bucket, remoteFilePath).get(0), tmpFile);

                    if (bucketFile.length() >= tmpFile.length()) {

                        log.info("\tappending to local file: " + bucketFile.getName());
                        AvroUtils.appendToLocalFile(tmpFile.getPath(), bucketFile.getPath(), true);

                        log.info("\tuploading local file: " + bucketFile.getName());
                        s3Service.uploadLocalFile(s3Bucket, remoteFilePath, bucketFile, true);

                    } else {

                        log.info("\tappending to local file: " + tmpFile.getName());
                        AvroUtils.appendToLocalFile(bucketFile.getPath(), tmpFile.getPath(), true);

                        log.info("\tuploading local file: " + tmpFile.getName());
                        s3Service.uploadLocalFile(s3Bucket, remoteFilePath, tmpFile, true);
                    }

                } else {

                    log.info("\tuploading local file: " + bucketFile.getName());
                    s3Service.uploadLocalFile(s3Bucket, remoteFilePath, bucketFile, true);

                }

                log.info("\tuploading success tag file");
                s3Service.uploadLocalFile(s3Bucket, dirLocation.append("_SUCCESS").toString(), tagFile, true);

            }
            log.info("ingested files for " + vendor + " uploaded");

            for (File bucketFile : bucketFiles.values()) {

                FileUtils.deleteQuietly(bucketFile);

            }

            for (File tmpBucketFile: tmpBucketFiles) {

                FileUtils.deleteQuietly(tmpBucketFile);

            }
        }

        FileUtils.deleteQuietly(tagFile);

    }

    public void ingest() {

        if (prevIngestionMillis == 0) {

            log.info("datacloud ingestion service starts...");

        }
        long curMillis = System.currentTimeMillis();

        try {

            if (prevCollectMillis == 0 || curMillis - prevCollectMillis > 3600 * 1000) {

                prevCollectMillis = curMillis;

            }

            //get consumed workers
            List<String> statusList = Collections.singletonList(CollectionWorker.STATUS_CONSUMED);
            List<CollectionWorker> consumedWorkers = collectionWorkerService.getWorkerByStatus(statusList);
            if (CollectionUtils.isEmpty(consumedWorkers)) {

                log.info("no consumed worker to handle, quit...");
                return;

            }

            //process consumed workers
            int workerCount = consumedWorkers.size();
            log.info("there're " + workerCount + " consumed worker to ingest:");
            List<Boolean> retList = new ArrayList<>(workerCount);
            Map<String, Map<Long, File>> bucketGroups = new HashMap<>();
            for (int i = 0; i < workerCount; ++i) {

                CollectionWorker worker = consumedWorkers.get(i);

                boolean ret = ingestConsumedWorker(worker, bucketGroups);
                log.info("\t" + (i + 1) + "/" + workerCount + " done");

                retList.add(ret);
            }

            //upload bucket files
            uploadIngestedBucket(bucketGroups);

            //update status
            for (int i = 0; i < workerCount; ++i) {

                CollectionWorker worker = consumedWorkers.get(i);

                if (retList.get(i)) {
                    worker.setStatus(CollectionWorker.STATUS_INGESTED);
                    collectionWorkerService.getEntityMgr().update(worker);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public int getIngestionTaskCount() {

        List<String> statusList = Collections.singletonList(CollectionWorker.STATUS_CONSUMED);
        List<CollectionWorker> consumedWorkers = collectionWorkerService.getWorkerByStatus(statusList);

        return CollectionUtils.size(consumedWorkers);

    }

    //0 -> Sun, 1 -> Mon, ...
    private static int getWeekDay(long millis) {

        int dayInMS = 86400 * 1000;
        int weekInMS = dayInMS * 7;
        millis -= millis % dayInMS;
        return ((int)(millis % weekInMS) / dayInMS + 4) % 7;

    }

    public void consolidate(List<String> effectiveVendors) {

        /*
        if (consolidationExecPeriod == 86400 * 7) {

            int weekDay = getWeekDay(System.currentTimeMillis());
            if (weekDay != consolidationExecWeekDay) {

                return;

            }

        }*/

        //current workers ingested
        List<String> statusList = Collections.singletonList(CollectionWorker.STATUS_INGESTED);
        List<CollectionWorker> consumedWorkers = collectionWorkerService.getWorkerByStatus(statusList);

        if (CollectionUtils.size(consumedWorkers) == 0) {

            log.warn("Notice: there're 0 ingested worker output to consolidate...");

        }


        //current time stamp
        long now = System.currentTimeMillis();

        try {

            Set<String> external_vendor_set = effectiveVendors == null ? null : new HashSet<>(effectiveVendors);

            for (VendorConfig vendorConfig: vendorConfigMgr.findAll()) {

                String vendor = vendorConfig.getVendor();

                //only handling effective vendors
                if (!VendorConfig.EFFECTIVE_VENDOR_SET.contains(vendor) ||
                    external_vendor_set != null && !external_vendor_set.contains(vendor)) {

                    continue;

                }

                //check consolidation period
                Timestamp ts = vendorConfig.getLastConsolidated();
                /*
                boolean triggerConsolidation =
                        ts == null || (now - ts.getTime()) / 1000 >= consolidationExecPeriod;
                if (!triggerConsolidation) {

                    continue;

                }*/

                //log prelude
                log.info("issue consolidation request to datacloudapi for " + vendor);
                if (vendorConfig.getLastConsolidated() != null){

                    log.info("last time consolidating " + vendor + " is: " + ts);

                }
                else {

                    log.info(vendor + " has never been consolidated before");

                }

                //issue req
                ConsolidateCollectionConfig collectionConfig = new ConsolidateCollectionConfig();
                collectionConfig.setShouldInheritSchemaProp(false);
                collectionConfig.setTransformer("TransformerBase");
                collectionConfig.setVendor(vendor);
                collectionConfig.setRawIngestion(vendor + "_RAW");

                TransformationStepConfig stepConfig = new TransformationStepConfig();
                stepConfig.setTransformer("consolidateCollectionTransformer");
                stepConfig.setTargetSource("Consolidated" + vendor);
                stepConfig.setStepType("Simple");
                stepConfig.setNoInput(true);
                stepConfig.setConfiguration(JsonUtils.serialize(collectionConfig));

                PipelineTransformationRequest req = new PipelineTransformationRequest();
                req.setSubmitter("DataCloudService");
                req.setName("consolidateCollection");
                req.setSteps(Arrays.asList(stepConfig));

                TransformationProgress progress =  transformationProxy.transform(req, HdfsPodContext.getDefaultHdfsPodId());

                vendorConfig.setLastConsolidated(new Timestamp(System.currentTimeMillis()));
                vendorConfigMgr.update(vendorConfig);

                //log postlude
                log.info("consolidate req issued: \n" + progress);
            }

        } catch (Exception e) {
            log.error("exception occurred: " + e.getMessage());
            log.error("exception call stack: \n" + e.getStackTrace());
        }
    }

    @Override
    public void cleanup(Timestamp start, Timestamp end) {
        List<CollectionWorker> workers = collectionWorkerService.getWorkerBySpawnTimeBetween(start, end);

        // clean up the input and output csv
        for (CollectionWorker worker : workers) {
            String prefix = s3BucketPrefix + worker.getWorkerId();
            s3Service.cleanupPrefix(s3Bucket, prefix);
        }
        for (String vendor : VendorConfig.EFFECTIVE_VENDOR_SET) {
            String ingestionPrefix = S3PathBuilder.constructIngestionDir(vendor + "_RAW").toString();
            s3Service.cleanupPrefixByDateBetween(s3Bucket, ingestionPrefix, start, end);
        }

        rawCollectionRequestService.cleanupRequestsBetween(start, end);
        collectionRequestService.cleanupRequestsBetween(start, end);
        collectionWorkerService.cleanupWorkerBetween(start, end);
    }
}
