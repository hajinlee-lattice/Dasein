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
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

import javafx.util.Pair;

@Component
public class CollectionDBServiceImpl implements CollectionDBService {
    private static final Logger log = LoggerFactory.getLogger(CollectionDBServiceImpl.class);
    private static final int LATENCY_GAP_MS = 3000;
    @Inject
    RawCollectionRequestService rawCollectionRequestService;
    @Inject
    CollectionWorkerService collectionWorkerService;
    @Inject
    CollectionRequestService collectionRequestService;
    @Inject
    S3Service s3Service;
    @Inject
    ECSService ecsService;
    @Inject
    VendorConfigService vendorConfigService;
    //@Inject
    //AmazonECS ecsClient;
    //@Inject
    //AmazonECR ecrClient;
    //@Inject
    //AmazonS3 s3Client;
    @Value("${datacloud.collection.s3bucket}")
    String s3Bucket;
    @Value("${datacloud.collection.s3bucket.prefix}")
    String s3BucketPrefix;
    @Value("${aws.region}")
    String awsRegion;
    @Value("${datacloud.collection.ecr.image.name}")
    String ecrImageName;
    @Value("${datacloud.collection.ecs.cluster.name}")
    String ecsClusterName;
    @Value("${datacloud.collection.ecs.task.def.name}")
    String ecsTaskDefName;
    @Value("${datacloud.collection.ecs.task.cpu}")
    String ecsTaskCpu;
    @Value("${datacloud.collection.ecs.task.memory}")
    String ecsTaskMemory;
    @Value("${datacloud.collection.ecs.task.subnets}")
    String ecsTaskSubnets;
    @Inject
    YarnConfiguration yarnConfiguration;
    @Inject
    HdfsPathBuilder hdfsPathBuilder;

    @Value("${datacloud.collection.bw.timestamp}")
    String bwTimestampColumn;
    @Value("${datacloud.collection.ingestion.partion.period}")
    int ingestionPartionPeriod;

    public boolean addNewDomains(List<String> domains, String vendor, String reqId) {
        return rawCollectionRequestService.addNewDomains(domains, vendor, reqId);
    }

    public void addNewDomains(List<String> domains, String reqId) {
        List<String> vendors = vendorConfigService.getVendors();
        for (int i = 0; i < vendors.size(); ++i)
            addNewDomains(domains, vendors.get(i), reqId);
    }

    public int transferRawRequests(boolean deleteFilteredReqs) {
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
            for (int i = 0; i < readyReqs.size(); ++i) {
                writer.write(readyReqs.get(i).getDomain());
                writer.newLine();
            }
        }

        return tempFile;
    }

    private Map<String, String> constructLogOptions() {
        HashMap<String, String> ret = new HashMap<String, String>();
        ret.put("awslogs-group", "/ecs/datacloud-collector");
        ret.put("awslogs-region", awsRegion);
        ret.put("awslogs-stream-prefix", "ecs");
        return ret;
    }

    /*
    private String spawnECSTask(String clusterName,
                                String taskDefName,
                                String imageName,
                                String cmdLine,
                                String workerId) throws Exception {
        String repoEndpoint = ecrClient.getAuthorizationToken(new GetAuthorizationTokenRequest()).getAuthorizationData().get(0).getProxyEndpoint().replace("https://", "");
        String dockerImageRef = repoEndpoint + "/" + imageName + ":latest";

        CreateClusterResult ret0 = ecsClient.createCluster(new CreateClusterRequest().withClusterName(clusterName));
        log.info("creating ECS cluster done: " + ret0.getCluster().getClusterName());

        RegisterTaskDefinitionResult ret1 = ecsClient.registerTaskDefinition(new RegisterTaskDefinitionRequest()
                .withFamily(taskDefName)
                .withNetworkMode(NetworkMode.Awsvpc)
                .withTaskRoleArn("ecsTaskExecutionRole")
                .withExecutionRoleArn("ecsTaskExecutionRole")
                .withRequiresCompatibilities(Compatibility.FARGATE)
                .withCpu(ecsTaskCpu)
                .withMemory(ecsTaskMemory)
                .withContainerDefinitions(new ContainerDefinition()
                        .withName("python")
                        .withImage(dockerImageRef)
                        .withCpu(Integer.valueOf(ecsTaskCpu))
                        .withMemory(Integer.valueOf(ecsTaskMemory))
                        .withCommand(cmdLine).withLogConfiguration(new LogConfiguration()
                                .withLogDriver(LogDriver.Awslogs)
                                .withOptions(constructLogOptions()))));
        log.info("registering task definition done: " + ret1.getTaskDefinition().getFamily());

        RunTaskResult ret2 = ecsClient.runTask(new RunTaskRequest()
                .withCluster(clusterName)
                .withTaskDefinition(taskDefName)
                .withCount(1)
                .withLaunchType(LaunchType.FARGATE)
                .withStartedBy(workerId)
                .withOverrides(new TaskOverride()
                        .withContainerOverrides(new ContainerOverride()
                                .withName("python")
                                .withEnvironment(new KeyValuePair()
                                        .withName("DATA_CLOUD_USE_CONSUL")
                                        .withValue("true"))))
                .withNetworkConfiguration(new NetworkConfiguration()
                        .withAwsvpcConfiguration(new AwsVpcConfiguration()
                                .withSubnets(ecsTaskSubnets.split(","))
                                .withAssignPublicIp(AssignPublicIp.DISABLED))));

        List<Failure> failures = ret2.getFailures();
        if (failures.size() != 0) {
            log.error("running task request failed: ");
            for (int i = 0; i < failures.size(); ++i) {
                Failure failure = failures.get(i);
                log.error("\t", failure.getArn(), failure.getReason());
            }

            throw new Exception("trying to run task on ECS cluster failed");
        } else {
            log.info("running task request issued: ");
            List<Task> tasks = ret2.getTasks();
            for (int i = 0; i < tasks.size(); ++i) {
                Task task = tasks.get(i);
                log.info("\t", task.getTaskArn(), task.getLastStatus());
            }
        }

        return ret2.getTasks().get(0).getTaskArn();
    }*/

    public int spawnCollectionWorker() throws Exception {
        int spawnedTasks = 0;
        List<String> vendors = vendorConfigService.getVendors();
        int maxRetries = vendorConfigService.getDefMaxRetries();
        int collectingBatch = vendorConfigService.getDefCollectionBatch();
        for (int i = 0; i < vendors.size(); ++i) {
            String vendor = vendors.get(i);

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
            if (activeTasks >= taskLimit)
                continue;

            //get ready reqs
            List<CollectionRequest> readyReqs = collectionRequestService.getReady(vendor, collectingBatch);
            if (readyReqs == null || readyReqs.size() == 0)
                continue;

            //generate input csv
            File tempCsv = generateCSV(readyReqs);

            //generate worker id
            String workerId = UUID.randomUUID().toString().toUpperCase();

            //upload to s3
            String prefix = s3BucketPrefix + workerId + "/input/domains.csv";
            s3Service.uploadLocalFile(s3Bucket, prefix, tempCsv, true);
            tempCsv.delete();

            //spawn worker in aws, '-v vendor -w worker_id'
            String cmdLine = "-v " + vendor + " -w " + workerId;
            /*String taskArn = ecsService.spawECSTask(
                    ecsClusterName, "python", vendor, ecrImageName,
                    cmdLine, workerId, "ecsTaskExecutionRole", "ecsTaskExecutionRole",
                    Integer.valueOf(ecsTaskCpu), Integer.valueOf(ecsTaskMemory),
                    ecsTaskSubnets,
                    new LogConfiguration()
                            .withLogDriver(LogDriver.Awslogs)
                            .withOptions(constructLogOptions()),
                    new KeyValuePair()
                            .withName("DATA_CLOUD_USE_CONSUL")
                            .withValue("true"));*/
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
        if (spawnedTasks > 0)
            log.info("SPAWN_COLLECTION_WORKER=" + spawnedTasks);

        return spawnedTasks;
    }

    private Map<String, Task> getTasksByWorkers(List<CollectionWorker> workers) {
        List<String> taskArns = new ArrayList<String>(workers.size());
        for (int i = 0; i < workers.size(); ++i)
            taskArns.add(workers.get(i).getTaskArn());

        List<Task> tasks = ecsService.getTasks(ecsClusterName, taskArns);
        HashMap<String, Task> arn2tasks = new HashMap<>(tasks.size() * 2);
        for (int i = 0; i < tasks.size(); ++i) {
            Task task = tasks.get(i);
            arn2tasks.put(task.getTaskArn(), task);
        }

        return arn2tasks;
    }

    /*
    private long getDomainFromCsv(String vendor, File csvFile, Set<String> domains) throws Exception {
        long ret = 0;
        String domainField = CollectionDBUtil.getDomainField(vendor);
        String domainCheckField = CollectionDBUtil.getDomainCheckField(vendor);

        try (CSVReader csvReader = new CSVReader(new FileReader(csvFile))) {
            String[] header = csvReader.readNext();
            int domainIdx = -1, domainChkIdx = -1;
            for (int i = 0; i < header.length; ++i) {
                if (header[i].equals(domainField))
                    domainIdx = i;
                if (header[i].equals(domainCheckField))
                    domainChkIdx = i;
            }
            if (domainChkIdx == -1 || domainIdx == -1)
                return 0;

            String[] line = null;
            while ((line = csvReader.readNext()) != null) {
                ++ret;
                if (!line[domainChkIdx].equals(""))
                    domains.add(line[domainIdx]);
            }

            return ret;
        }
    }*/

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
                if (domainIdx == -1 || domainChkIdx == -1)
                    return ret;

                Iterator<CSVRecord> ite = parser.iterator();
                while (ite.hasNext()) {
                    ++ret;

                    CSVRecord rec = ite.next();
                    if (!rec.get(domainChkIdx).equals(""))
                        domains.add(rec.get(domainIdx));
                }

                return ret;
            }
        }
    }

    private boolean handleFinishedTask(CollectionWorker worker) throws Exception {
        String workerId = worker.getWorkerId();
        String vendor = worker.getVendor();

        //list file in s3 output path
        String prefix = s3BucketPrefix + workerId + "/output/";
        List<S3ObjectSummary> itemDescs = s3Service.listObjects(s3Bucket, prefix);
        //s3Client.listObjects(s3Bucket, prefix).getObjectSummaries();
        if (itemDescs.size() < 1)
            return false;

        //download content
        List<File> tmpFiles = new ArrayList<>(itemDescs.size());
        for (int i = 0; i < itemDescs.size(); ++i) {
            S3ObjectSummary itemDesc = itemDescs.get(i);
            if (itemDesc.getSize() == 0)
                continue;

            File tmpFile = File.createTempFile("temp", ".csv");
            tmpFile.deleteOnExit();
            tmpFiles.add(tmpFile);

            s3Service.downloadS3File(itemDesc, tmpFile);
        }

        if (tmpFiles.size() == 0)
            return false;

        //copy to hdfs
        String hdfsDir = hdfsPathBuilder.constructCollectorWorkerDir(vendor, workerId).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
        for (int i = 0; i < tmpFiles.size(); ++i)
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, tmpFiles.get(i).getPath(), hdfsDir);

        //parse csv
        HashSet<String> domains = new HashSet<>();
        long recordsCollected = 0;
        for (int i = 0; i < tmpFiles.size(); ++i) {
            recordsCollected += getDomainFromCsvEx(vendor, tmpFiles.get(i), domains);
        }
        worker.setRecordsCollected(recordsCollected);
        log.info("END_COLLECTING_REQ=" + vendor + "," + domains.size() + "," + recordsCollected);

        //consumeFinished reqs
        collectionRequestService.consumeFinished(workerId, domains);
        domains.clear();

        //clean tmp files
        for (int i = 0; i < tmpFiles.size(); ++i)
            tmpFiles.get(i).delete();

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
    public int updateCollectingStatus() throws Exception {
        int stoppedTasks = 0;
        List<String> statusList = new ArrayList<>(2);
        statusList.add(CollectionWorker.STATUS_NEW);
        statusList.add(CollectionWorker.STATUS_RUNNING);
        statusList.add(CollectionWorker.STATUS_FINISHED);
        List<CollectionWorker> activeWorkers = collectionWorkerService.getWorkerByStatus(statusList);
        if (activeWorkers == null || activeWorkers.size() == 0)
            return 0;

        //handling active worker/task
        Map<String, Task> activeTasks = getTasksByWorkers(activeWorkers);
        if (activeTasks == null || activeTasks.size() == 0)
            return 0;

        int failedTasks = 0;
        for (int i = 0; i < activeWorkers.size(); ++i) {
            CollectionWorker worker = activeWorkers.get(i);
            Task task = activeTasks.get(worker.getTaskArn());

            if (task != null) {
                if (worker.getStatus().equals(CollectionWorker.STATUS_NEW) &&
                        (task.getLastStatus().equals("RUNNING") || task.getLastStatus().equals("STOPPED"))) {
                    log.info("task " + worker.getWorkerId() + " starts running");
                    worker.setStatus(CollectionWorker.STATUS_RUNNING);
                    collectionWorkerService.getEntityMgr().update(worker);
                }

                if (!task.getLastStatus().equals("STOPPED"))
                    continue;
            } else if (!worker.getStatus().equals(CollectionWorker.STATUS_FINISHED)) {
                log.info("task " + worker.getWorkerId() + " loses traces, mark it as finished now...");
                worker.setStatus(CollectionWorker.STATUS_FINISHED);
            }

            //transfer state to finished
            if (worker.getStatus().equals(CollectionWorker.STATUS_RUNNING)) {
                log.info("task " + worker.getWorkerId() + " finished running");
                worker.setStatus(CollectionWorker.STATUS_FINISHED);
                collectionWorkerService.getEntityMgr().update(worker);
            }

            //consuming output
            //download csv
            //Files.createTempDirectory()
            //no csv file: status => fail
            //copy csv file to hdfs
            if (worker.getStatus().equals(CollectionWorker.STATUS_FINISHED)) {
                log.info("task " + worker.getWorkerId() + " finished, starts consuming its output");
                boolean succ = handleFinishedTask(worker);

                //status => consumed/failed
                worker.setTerminationTime(task != null ? new Timestamp(task.getStoppedAt().getTime()) : new Timestamp
                        (System.currentTimeMillis()));
                worker.setStatus(succ ? CollectionWorker.STATUS_CONSUMED : CollectionWorker.STATUS_FAILED);
                collectionWorkerService.getEntityMgr().update(worker);

                ++stoppedTasks;
                if (!succ)
                    ++failedTasks;
                log.info("task " + worker.getWorkerId() + (succ ? " consumed" : " failed"));
            }
        }

        if (stoppedTasks > 0)
            log.info("COLLECTION_WORKER_STOPPED=" + stoppedTasks + "," + failedTasks);

        return stoppedTasks;
    }

    public int getActiveTaskCount() {
        List<String> statusList = new ArrayList<>(2);
        statusList.add(CollectionWorker.STATUS_NEW);
        statusList.add(CollectionWorker.STATUS_RUNNING);
        statusList.add(CollectionWorker.STATUS_FINISHED);
        List<CollectionWorker> activeWorkers = collectionWorkerService.getWorkerByStatus(statusList);

        return activeWorkers == null ? 0 : activeWorkers.size();
    }

    private long prevMillis = 0;
    private int prevActiveTasks = 0;

    public void service() {
        if (prevMillis == 0)
            log.info("datacloud collection job starts...");
        long currentMillis = System.currentTimeMillis();

        try {
            int activeTasks = getActiveTaskCount();
            if (prevMillis == 0 || prevActiveTasks != activeTasks || currentMillis - prevMillis >= 3600 * 1000) {
                prevMillis = currentMillis;
                log.info("There're " + activeTasks + " tasks");
            }
            prevActiveTasks = activeTasks;

            int reqs = transferRawRequests(true);
            if (reqs > 0)
                log.info(reqs + " requests transferred from raw requests to collection requests");
            Thread.sleep(LATENCY_GAP_MS);

            int newTasks = spawnCollectionWorker();
            activeTasks += newTasks;
            if (newTasks > 0)
                log.info(newTasks + " new collection workers spawned, active tasks => " + activeTasks);
            Thread.sleep(LATENCY_GAP_MS);

            int stoppedTasks = updateCollectingStatus();
            activeTasks -= stoppedTasks;
            if (stoppedTasks > 0)
                log.info(stoppedTasks + " tasks stopped, active tasks => " + activeTasks);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private String getTimestampColumn(String vendor) throws Exception {
        switch (vendor) {
            case "BUILTWITH":
                return bwTimestampColumn;
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
        for (int i = 0; i < inputFiles.size(); ++i) {
            File file = inputFiles.get(i);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                try (CSVParser parser = new CSVParser(reader, format)) {
                    Map<String, Integer> colMap = parser.getHeaderMap();
                    int tsIdx = colMap.getOrDefault(tsCol, -1);
                    if (tsIdx == -1)
                        continue;

                    Iterator<CSVRecord> ite = parser.iterator();
                    while (ite.hasNext()) {
                        String tsText = ite.next().get(tsIdx);
                        try {
                            long tsVal = Long.parseLong(tsText);
                            if (tsVal > maxTs)
                                maxTs = tsVal;
                            if (tsVal < minTs)
                                minTs = tsVal;
                        } catch (Exception e) {
                            log.error(tsText + " is not a valid timestamp value");
                            continue;
                        }
                    }
                }
            }
        }

        if (minTs == Long.MAX_VALUE || maxTs == Long.MIN_VALUE) {
            log.error("timestamp range wrong: [" + minTs + ", " + maxTs + "]");
            throw new Exception("wrong timestamp range");
        }
        return new Pair<>(minTs, maxTs);
    }

    private Schema getSchema(String vendor) throws Exception {
        switch (vendor) {
            case "BUILTWITH":
                return new Schema.Parser().parse(AvroUtils.buildSchema("builtwith.avsc"));
            default:
                throw new Exception("not implemented");
        }
    }

    private static final int BUCKET_CACHE_LIMIT = 1024;

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
        long minTs = tsPair.getKey(), maxTs = tsPair.getValue();
        int periodInMS = ingestionPartionPeriod * 1000;
        if (minTs % periodInMS != 0)
            minTs -= minTs % periodInMS;

        //create bucket file/buffers
        int bucketCount = 1 + (int) ((maxTs - minTs) / periodInMS);
        List<File> bucketFiles = createBucketFiles(minTs, periodInMS, bucketCount);
        boolean[] bucketCreated = new boolean[bucketCount];
        List<List<GenericRecord>> bucketBuffers = new ArrayList<>();
        for (int i = 0; i < bucketCount; ++i)
            bucketBuffers.add(new ArrayList<>());

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
                    if (fields.size() != columnCount)
                        throw new Exception("avro column count != csv column count");
                    for (int i = 0; i < columnCount; ++i) {
                        Schema.Field field = fields.get(i);
                        int csvIdx = csvColName2Idx.get(field.name());
                        csvIdx2Avro[csvIdx] = field.pos();
                        avroIdx2Fields[csvIdx] = field;
                    }
                    int tsAvroCol = csvIdx2Avro[csvColName2Idx.get(tsColName)];

                    //iterate csv, generate avro
                    Iterator<CSVRecord> ite = parser.iterator();
                    while (ite.hasNext()) {
                        CSVRecord csvRec = ite.next();
                        if (domainChkCsvCol != -1 && csvRec.get(domainChkCsvCol).equals("")) //bypass dummy line
                            continue;

                        //create avro record, add it to buffer
                        GenericRecord rec = new GenericData.Record(schema);
                        for (int i = 0; i < columnCount; ++i)
                            rec.put(csvIdx2Avro[i], AvroUtils.checkTypeAndConvert(avroIdx2Fields[i].name(), csvRec.get(i), avroIdx2Fields[i].schema().getType()));

                        int bucketIdx = (int) (((Long) rec.get(tsAvroCol) - minTs) / periodInMS);
                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        buf.add(rec);

                        //flush bucket buffer when it's full
                        if (buf.size() == BUCKET_CACHE_LIMIT) {
                            if (!bucketCreated[bucketIdx]) {
                                AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketIdx).getPath(), true);
                                bucketCreated[bucketIdx] = true;
                            } else
                                AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketIdx).getPath(), true);

                            buf.clear();
                        }
                    }

                    //flush non-empty buffers
                    for (int bucketIdx = 0; bucketIdx < bucketCount; ++bucketIdx) {
                        List<GenericRecord> buf = bucketBuffers.get(bucketIdx);
                        if (buf.size() == 0)
                            continue;

                        if (!bucketCreated[bucketIdx]) {
                            AvroUtils.writeToLocalFile(schema, buf, bucketFiles.get(bucketIdx).getPath(), true);
                            bucketCreated[bucketIdx] = true;
                        } else
                            AvroUtils.appendToLocalFile(buf, bucketFiles.get(bucketIdx).getPath(), true);

                        buf.clear();
                    }
                }
            }
        }

        return bucketFiles;
    }

    private void ingestConsumedWorker(CollectionWorker worker) throws Exception {
        String vendor = worker.getVendor();

        //hdfs path contain the collection result
        String hdfsDir = hdfsPathBuilder.constructCollectorWorkerDir(vendor, worker.getWorkerId()).toString();
        List<String> hdfsFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, ".+\\.csv");
        if (hdfsFiles == null || hdfsFiles.size() == 0) {
            log.error(worker.getVendor() + " worker " + worker.getWorkerId() + "\'s output dir on hdfs does not contain csv files");
            return;
        }

        //copy hdfs file to local for processing
        List<File> tmpFiles = new ArrayList<>();
        for (int i = 0; i < hdfsFiles.size(); ++i) {
            File file = File.createTempFile("ingest", "csv");
            file.deleteOnExit();
            tmpFiles.add(file);

            HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsFiles.get(i), file.getPath());
        }

        //bucketing
        List<File> bucketFiles = doBucketing(tmpFiles, vendor);
        if (bucketFiles != null && bucketFiles.size() > 0) {
            String hdfsIngestDir = hdfsPathBuilder.constructIngestionDir(vendor + "_RAW").toString();
            HdfsUtils.mkdir(yarnConfiguration, hdfsIngestDir);

            for (int i = 0; i < bucketFiles.size(); ++i) {
                File bucketFile = bucketFiles.get(i);
                String hdfsFilePath = hdfsIngestDir + bucketFile.getName();
                if (HdfsUtils.fileExists(yarnConfiguration, hdfsFilePath))
                    AvroUtils.appendToHdfsFile(yarnConfiguration, hdfsFilePath,
                            AvroUtils.readFromLocalFile(bucketFile.getPath()), true);
                else
                    HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, bucketFile.getPath(), hdfsIngestDir);
            }
        }

        //clean local file
        for (int i = 0; i < tmpFiles.size(); ++i)
            tmpFiles.get(i).delete();
        for (int i = 0; i < bucketFiles.size(); ++i)
            bucketFiles.get(i).delete();

        //update worker status
        worker.setStatus(CollectionWorker.STATUS_INGESTED);
        collectionWorkerService.getEntityMgr().update(worker);
    }

    private long prevIngestionMillis = 0;

    public void ingest() {
        if (prevIngestionMillis == 0)
            log.info("datacloud ingestion service starts...");
        long curMillis = System.currentTimeMillis();

        try {
            if (prevMillis == 0 || curMillis - prevMillis > 3600 * 1000) {
                prevMillis = curMillis;
            }

            //get consumed workers
            List<String> statusList = new ArrayList<>(2);
            statusList.add(CollectionWorker.STATUS_CONSUMED);
            List<CollectionWorker> consumedWorkers = collectionWorkerService.getWorkerByStatus(statusList);
            if (consumedWorkers == null || consumedWorkers.size() == 0)
                return;

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
        List<String> statusList = new ArrayList<>(2);
        statusList.add(CollectionWorker.STATUS_CONSUMED);
        List<CollectionWorker> consumedWorkers = collectionWorkerService.getWorkerByStatus(statusList);

        return consumedWorkers == null ? 0 : consumedWorkers.size();
    }
}
