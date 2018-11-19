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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.ecs.ECSService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.collection.service.CollectionDBService;
import com.latticeengines.datacloud.collection.service.CollectionRequestService;
import com.latticeengines.datacloud.collection.service.CollectionWorkerService;
import com.latticeengines.datacloud.collection.service.RawCollectionRequestService;
import com.latticeengines.datacloud.collection.service.VendorConfigService;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;

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
    private VendorConfigService vendorConfigService;

    @Inject
    private YarnConfiguration yarnConfiguration;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

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

    private long prevCollectMillis = 0;
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

        List<String> vendors = vendorConfigService.getVendors();
        int maxRetries = vendorConfigService.getDefMaxRetries();
        int collectingBatch = vendorConfigService.getDefCollectionBatch();
        for (String vendor : vendors) {

            //fixme: is the earliest time of collecting reqs enough? may be ready reqs should also be considered?
            //get earliest active request time
            Timestamp earliestReqTime = collectionRequestService.getEarliestTime(vendor, CollectionRequest.STATUS_COLLECTING);

            //get stopped worker since that time
            List<CollectionWorker> stoppedWorkers = collectionWorkerService.getWorkerStopped(vendor, earliestReqTime);

            //modify req status to READY | FAILED based on retry times
            int modified = collectionRequestService.handlePending(vendor, maxRetries, stoppedWorkers);
            if (modified > 0) {

                log.info("find " + stoppedWorkers.size() + " workers recently stopped");
                log.info(modified + " pending collection requests reset to ready");

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

    private long getDomainFromCsvEx(String vendor, File csvFile, Set<String> domains) throws Exception {

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

                    if (!rec.get(domainChkIdx).equals("")) {

                        domains.add(rec.get(domainIdx));

                    }

                }

                return ret;

            }

        }

    }

    private boolean handleFinishedTask(CollectionWorker worker) throws Exception {

        String workerId = worker.getWorkerId();

        //list file in s3 output path
        String prefix = s3BucketPrefix + workerId + "/output/";
        List<S3ObjectSummary> itemDescs = s3Service.listObjects(s3Bucket, prefix);
        if (itemDescs.size() < 1) {

            return false;

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
        if (tmpFiles.size() == 0) {

            return false;

        }

        String vendor = worker.getVendor();

        //copy to hdfs
        String hdfsDir = hdfsPathBuilder.constructCollectorWorkerDir(vendor, workerId).toString();
        log.info("about to upload collected data to hdfs: " + hdfsDir);
        HdfsUtils.mkdir(yarnConfiguration, hdfsDir);

        for (File tmpFile : tmpFiles) {

            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, tmpFile.getPath(), hdfsDir);

        }

        //parse csv
        HashSet<String> domains = new HashSet<>();
        long recordsCollected = 0;

        for (File tmpFile : tmpFiles) {

            recordsCollected += getDomainFromCsvEx(vendor, tmpFile, domains);

        }

        worker.setRecordsCollected(recordsCollected);

        log.info("END_COLLECTING_REQ=" + vendor + "," + domains.size() + "," + recordsCollected);

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
        if (MapUtils.isEmpty(activeTasks)) {

            return 0;

        }

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

    public void collect() {

        if (prevCollectMillis == 0) {

            log.info("datacloud collection job starts...");

        }
        long currentMillis = System.currentTimeMillis();

        try {

            int activeTasks = getActiveTaskCount();
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

            }
            Thread.sleep(LATENCY_GAP_MS);

            int newTasks = spawnCollectionWorker();
            activeTasks += newTasks;
            if (newTasks > 0) {

                log.info(newTasks + " new collection workers spawned, active tasks => " + activeTasks);

            }
            Thread.sleep(LATENCY_GAP_MS);

            int stoppedTasks = updateCollectingStatus();
            activeTasks -= stoppedTasks;
            if (stoppedTasks > 0) {

                log.info(stoppedTasks + " tasks stopped, active tasks => " + activeTasks);

            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private String getTimestampColumn(String vendor) throws Exception {

        switch (vendor) {
            case VendorConfig.VENDOR_BUILTWITH:
                return bwTimestampColumn;
            case VendorConfig.VENDOR_ALEXA:
                return alexaTimestampColumn;
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
            default:
                throw new Exception("not implemented");
        }
    }

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
        if (minTs % periodInMS != 0) {

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
                    if (fields.size() != columnCount) {

                        throw new Exception("avro column count != csv column count");

                    }

                    for (Schema.Field field: fields) {

                        int csvIdx = csvColName2Idx.get(field.name());
                        csvIdx2Avro[csvIdx] = field.pos();
                        avroIdx2Fields[csvIdx] = field;

                    }
                    int tsAvroCol = csvIdx2Avro[csvColName2Idx.get(tsColName)];

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

            //hdfs path contain the collection result
            String hdfsDir = hdfsPathBuilder.constructCollectorWorkerDir(vendor, worker.getWorkerId()).toString();
            log.info("handling collected data in hdfs: " + hdfsDir);
            List<String> hdfsFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, ".+\\.csv");
            if (CollectionUtils.isEmpty(hdfsFiles)) {

                log.error(worker.getVendor() + " worker " + worker.getWorkerId() //
                        + "\'s output dir on hdfs does not contain csv files");
                return;

            }

            //copy hdfs file to local for processing
            List<File> tmpFiles = new ArrayList<>();
            for (String hdfsFile : hdfsFiles) {

                File file = File.createTempFile("ingest", "csv");
                file.deleteOnExit();
                tmpFiles.add(file);

                HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsFile, file.getPath());

            }

            //bucketing
            List<File> bucketFiles = doBucketing(tmpFiles, vendor);
            if (CollectionUtils.isNotEmpty(bucketFiles)) {

                Path hdfsIngestLocation = hdfsPathBuilder.constructIngestionDir(vendor + "_RAW");
                String hdfsIngestDir = hdfsIngestLocation.toString();
                HdfsUtils.mkdir(yarnConfiguration, hdfsIngestDir);

                for (File bucketFile : bucketFiles) {

                    if (!bucketFile.exists()) {

                        log.warn("no data gathered for bucket file: " + bucketFile.getPath());
                        continue;

                    }

                    String hdfsFilePath = hdfsIngestLocation.append(bucketFile.getName()).toString();

                    if (HdfsUtils.fileExists(yarnConfiguration, hdfsFilePath)) {

                        AvroUtils.appendToHdfsFile(yarnConfiguration, hdfsFilePath,
                                AvroUtils.readFromLocalFile(bucketFile.getPath()), true);

                        log.info("appending to hdfs file: " + hdfsFilePath);

                    } else {

                        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, bucketFile.getPath(), hdfsIngestDir);

                        log.info("creating hdfs file: " + hdfsFilePath);

                    }

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

            }

            //update worker status
            worker.setStatus(CollectionWorker.STATUS_INGESTED);
            collectionWorkerService.getEntityMgr().update(worker);

        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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

                return;

            }

            //process consumed workers
            int workerCount = consumedWorkers.size();
            log.info("there're " + workerCount + " consumed worker to ingest:");
            for (int i = 0; i < workerCount; ++i) {

                CollectionWorker worker = consumedWorkers.get(i);

                ingestConsumedWorker(worker);
                log.info("\t" + (i + 1) + "/" + workerCount + " done");

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
}
