package com.latticeengines.matchapi.controller;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

public class MatchCorrectnessDeploymentTestNG extends MatchapiDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(MatchCorrectnessDeploymentTestNG.class);
    private static final Integer REALTIME_THREADS = 32;
    private static final String AVRO_DIR = "/tmp/MatchCorrectnessDeploymentTestNG";
    private static final String HDFS_POD = "MatchCorrectnessDeploymentTestNG";
    private static final List<String> fields = Arrays.asList("Domain", "Name", "City", "State", "Country");
    private static final String BULK_VALUE = "bulkValue";
    private static final String REALTIME_VALUE = "realtimeValue";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    private List<List<Object>> accountPool;
    private Integer numFields;
    private List<Map<String, Object>> bulkResult;
    private List<Map<String, Object>> realtimeResult;

    @Value("${propdata.test.correctness.rows}")
    private Integer numRecords;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestDataCloudVersion;

    @Autowired
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @BeforeMethod(groups = "deployment")
    private void setUpMethod() {
        prepareCleanPod(HDFS_POD);
        loadAccountPool();
        bulkResult = new ArrayList<>();
        realtimeResult = new ArrayList<>();
    }

    @Test(groups = "deployment", dataProvider = "versions")
    public void testMatchCorrectness(String dataCloudVersion) {
        Integer numGoodAccounts = new Double(numRecords * 0.8).intValue();
        Integer numBadAccounts = numRecords - numGoodAccounts;

        List<List<Object>> data = getGoodAccounts(numGoodAccounts);
        data.addAll(getGarbageDomainAccounts(numBadAccounts));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Boolean> bulkFuture = executor.submit(new BulkMatchCallable(data, dataCloudVersion));
        Future<Boolean> realtimeFuture = executor.submit(new RealtimeMatchCallable(data, dataCloudVersion));

        try {
            realtimeFuture.get(3600L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to wait for realtime match to finish.", e);
        }

        try {
            bulkFuture.get(3600L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to wait for bulk match to finish.", e);
        }

        executor.shutdown();

        compareResults();
    }

    @DataProvider(name = "versions")
    public Object[][] versionsToTest() {
        return new Object[][] { { null }, { dataCloudVersionEntityMgr.latestApprovedForMajorVersion(latestDataCloudVersion) } };
    }

    private void compareResults() {
        int numBulkResults = bulkResult.size();
        int numRealTimeResults = realtimeResult.size();
        System.out.println(String.format("Got %d results in bulk match, %d in realtime match.", numBulkResults,
                numRealTimeResults));

        List<Map<String, Object>> missingRealtimeInputs = new ArrayList<>();
        List<Map<String, Object>> missingBulkInputs = new ArrayList<>();
        Integer numDifference = 0;
        // load reatime match to hash table
        Map<String, Map<String, Object>> realtimeMap = new HashMap<>();
        for (Map<String, Object> map : realtimeResult) {
            String hash = hashResultRecord(map);
            realtimeMap.put(hash, map);
        }

        // compare with bulk match
        Set<String> bulkMatchSet = new HashSet<>();
        for (Map<String, Object> map : bulkResult) {
            String hash = hashResultRecord(map);
            bulkMatchSet.add(hash);
            if (!realtimeMap.containsKey(hash)) {
                Map<String, Object> input = extractInput(map);
                missingRealtimeInputs.add(input);
                System.out.println("Realtime Match missed input " + JsonUtils.serialize(input));
            } else {
                Map<String, Object> map2 = realtimeMap.get(hash);
                if (!compareTwoRecords(map, map2)) {
                    numDifference++;
                }
            }
        }

        // find missing bulk match
        for (Map<String, Object> map : realtimeResult) {
            String hash = hashResultRecord(map);
            if (!bulkMatchSet.contains(hash)) {
                Map<String, Object> input = extractInput(map);
                missingBulkInputs.add(input);
                System.out.println("Bulk Match missed input " + JsonUtils.serialize(input));
            }
        }

        String summary = "Realtime Match missed " + missingRealtimeInputs.size() + " inputs.\n" + "Bulk Match missed "
                + missingBulkInputs.size() + " inputs.\n" + "Got different results on " + numDifference + " inputs.";

        Assert.assertTrue(missingBulkInputs.isEmpty() && missingRealtimeInputs.isEmpty() && numDifference == 0,
                summary);

        System.out.println(summary);
    }

    private String hashResultRecord(Map<String, Object> map) {
        List<String> tokens = new ArrayList<>();
        for (String inputField : fields) {
            tokens.add(String.valueOf(map.get(inputField)));
        }
        String token = StringUtils.join(tokens, "|");
        try {
            byte[] bytesOfMessage = token.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] thedigest = md.digest(bytesOfMessage);
            return Base64.encodeBase64URLSafeString(thedigest);
        } catch (Exception e) {
            throw new RuntimeException("Failed to hash result record", e);
        }
    }

    private Map<String, Object> extractInput(Map<String, Object> map) {
        Map<String, Object> input = new HashMap<>();
        for (String inputField : fields) {
            input.put(inputField, map.get(inputField));
        }
        return input;
    }

    private Boolean compareTwoRecords(Map<String, Object> bulkRecord, Map<String, Object> realtimeRecord) {
        Set<String> allFields = new HashSet<>(bulkRecord.keySet());
        allFields.addAll(realtimeRecord.keySet());
        Map<String, Map<String, Object>> mismatch = new HashMap<>();
        for (String field : allFields) {
            Object bulkValue = bulkRecord.get(field);
            Object realtimeValue = realtimeRecord.get(field);
            if ((bulkValue == null && realtimeValue == null) || (bulkValue != null && realtimeValue != null
                    && String.valueOf(bulkValue).equals(String.valueOf(realtimeValue)))) {
                continue;
            }
            Map<String, Object> map = new HashMap<>();
            map.put(BULK_VALUE, bulkValue);
            map.put(REALTIME_VALUE, realtimeValue);
            mismatch.put(field, map);
        }
        if (!mismatch.isEmpty()) {
            System.out.println("Got different results: " + JsonUtils.serialize(mismatch));
        }
        return mismatch.isEmpty();
    }

    private class RealtimeMatchCallable implements Callable<Boolean> {

        private Log log = LogFactory.getLog(RealtimeMatchCallable.class);
        private final List<List<Object>> data = new ArrayList<>();
        private ConcurrentLinkedDeque<Map<String, Object>> result = new ConcurrentLinkedDeque<>();
        private final String dataCloudVersion;

        RealtimeMatchCallable(List<List<Object>> data, String dataCloudVersion) {
            for (List<Object> row : data) {
                this.data.add(new ArrayList<>(row));
            }
            this.dataCloudVersion = dataCloudVersion;
        }

        @Override
        public Boolean call() {
            try {
                ExecutorService executor = Executors.newFixedThreadPool(REALTIME_THREADS);

                int count = 0;
                List<Future<MatchOutput>> futures = new ArrayList<>();
                for (List<Object> row: data) {
                    Future<MatchOutput> future = singleRun(row, executor);
                    futures.add(future);
                }

                for (Future<MatchOutput> future: futures) {
                    MatchOutput output = future.get();
                    if (output != null) {
                        readMatchOutput(output);
                    }
                    log.info("finished " + (++count) + " out of " + data.size() + " matches.");
                }
                realtimeResult.addAll(result);
                log.info("Loaded real time match result.");
            } catch (Exception e) {
                log.error(e);
                return false;
            }
            return true;
        }

        private Future<MatchOutput> singleRun(List<Object> row, ExecutorService executor) {
            final MatchInput input = new MatchInput();
            input.setReturnUnmatched(true);
            input.setPredefinedSelection(Predefined.RTS);
            input.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
            input.setFields(fields);
            input.setData(Collections.singletonList(row));
            input.setDataCloudVersion(dataCloudVersion);
            return executor.submit(new SingleRun(input));
        }

        private void readMatchOutput(MatchOutput matchOutput) {
            List<Map<String, Object>> newResults = new ArrayList<>();
            List<String> inputFields = matchOutput.getInputFields();
            List<String> outputFields = matchOutput.getOutputFields();
            for (OutputRecord record : matchOutput.getResult()) {
                if (record.getOutput() != null && !record.getOutput().isEmpty()) {
                    Map<String, Object> map = new HashMap<>();
                    for (int i = 0; i < inputFields.size(); i++) {
                        map.put(inputFields.get(i), record.getInput().get(i));
                    }
                    for (int i = 0; i < outputFields.size(); i++) {
                        map.put(outputFields.get(i), record.getOutput().get(i));
                    }
                    newResults.add(map);
                }
            }
            result.addAll(newResults);
            log.info("loaded " + result.size() + " real time match results.");
        }

        private class SingleRun implements Callable<MatchOutput> {

            private Log log = LogFactory.getLog(SingleRun.class);
            private MatchInput matchInput;

            SingleRun(MatchInput matchInput) {
                this.matchInput = matchInput;
            }

            @Override
            public MatchOutput call() {
                try {
                    return matchProxy.matchRealTime(matchInput);
                } catch (Exception e) {
                    log.error(e);
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private class BulkMatchCallable implements Callable<Boolean> {

        private Log log = LogFactory.getLog(BulkMatchCallable.class);
        private final List<List<Object>> data = new ArrayList<>();
        private final String dataCloudVersion;

        BulkMatchCallable(List<List<Object>> data, String dataCloudVersion) {
            for (List<Object> row : data) {
                this.data.add(new ArrayList<>(row));
            }
            this.dataCloudVersion = dataCloudVersion;
        }

        @Override
        public Boolean call() {
            try {
                HdfsPodContext.changeHdfsPodId(HDFS_POD);
                upload();
                MatchInput input = constructMatchInput();
                MatchCommand matchCommand = matchProxy.matchBulk(input, HDFS_POD);
                matchCommand = waitForMatchCommand(matchCommand);
                bulkResult.addAll(readResult(matchCommand));
                log.info("Loaded bulk match result.");
            } catch (Exception e) {
                log.error(e);
                return false;
            }
            return true;
        }

        private MatchInput constructMatchInput() {
            MatchInput matchInput = new MatchInput();
            matchInput.setReturnUnmatched(true);
            matchInput.setTenant(new Tenant(PropDataConstants.SERVICE_CUSTOMERSPACE));
            matchInput.setPredefinedSelection(Predefined.RTS);
            AvroInputBuffer inputBuffer = new AvroInputBuffer();
            inputBuffer.setAvroDir(AVRO_DIR);
            matchInput.setInputBuffer(inputBuffer);
            matchInput.setDataCloudVersion(dataCloudVersion);
            return matchInput;
        }

        private void upload() {
            log.info("Uploading test data to MatchInput.avro ...");
            List<Class<?>> fieldTypes = Arrays.asList((Class<?>) String.class, String.class, String.class, String.class,
                    String.class);
            uploadAvroData(data, fields, fieldTypes, AVRO_DIR, "MatchInput.avro");
        }

        private MatchCommand waitForMatchCommand(MatchCommand matchCommand) {
            String rootUid = matchCommand.getRootOperationUid();
            log.info(String.format("Waiting for match command %s to complete", rootUid));

            MatchStatus status;
            do {
                matchCommand = matchProxy.bulkMatchStatus(rootUid);
                status = matchCommand.getMatchStatus();
                if (status == null) {
                    throw new LedpException(LedpCode.LEDP_28024, new String[] { rootUid });
                }
                String logMsg = "Match Status = " + status;
                if (MatchStatus.MATCHING.equals(status)) {
                    Float progress = matchCommand.getProgress();
                    logMsg += String.format(": %.2f %%", progress * 100);
                }
                log.info(logMsg);

                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    // Ignore InterruptedException
                }

            } while (!status.isTerminal());

            if (!MatchStatus.FINISHED.equals(status)) {
                throw new IllegalStateException(
                        "The terminal status of match is " + status + " instead of " + MatchStatus.FINISHED);
            }

            return matchCommand;
        }

        private List<Map<String, Object>> readResult(MatchCommand matchCommand) {
            List<Map<String, Object>> toReturn = new ArrayList<>();
            String rootUid = matchCommand.getRootOperationUid();
            String outputDir = hdfsPathBuilder.constructMatchOutputDir(rootUid).toString();
            String avroGlobs = outputDir + (outputDir.endsWith("/") ? "*.avro" : "/*.avro");
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlobs);
            List<Schema.Field> fields = schema.getFields();
            Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, avroGlobs);
            while (iterator.hasNext()) {
                Map<String, Object> map = new HashMap<>();
                GenericRecord record = iterator.next();
                for (int i = 0; i < fields.size(); i++) {
                    String fieldName = fields.get(i).name();
                    if (fieldName.startsWith("Source_")) {
                        fieldName = fieldName.replaceFirst("Source_", "");
                    }
                    Object val = record.get(i);
                    if (val instanceof Utf8) {
                        map.put(fieldName, ((Utf8) val).toString());
                    } else {
                        map.put(fieldName, val);
                    }
                }
                toReturn.add(map);
            }
            return toReturn;
        }

    }

    private List<List<Object>> getGarbageDomainAccounts(int num) {
        List<List<Object>> data = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            List<Object> row = new ArrayList<>();
            row.add(generateGarbageDomain());
            for (int j = 0; j < numFields - 1; j++) {
                row.add(null);
            }
            data.add(row);
        }
        return data;
    }

    private String generateGarbageDomain() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 6) + ".com";
    }

    private List<List<Object>> getGoodAccounts(int num) {
        Random random = new Random(System.currentTimeMillis());
        int poolSize = accountPool.size();
        List<List<Object>> data = new ArrayList<>();
        Set<Integer> visitedRows = new HashSet<>();
        for (int i = 0; i < num; i++) {
            int randomPos = random.nextInt(poolSize);
            while (visitedRows.contains(randomPos)) {
                randomPos = random.nextInt(poolSize);
            }
            data.add(accountPool.get(randomPos));
            visitedRows.add(randomPos);
        }
        return data;
    }

    private void loadAccountPool() {
        if (accountPool != null) {
            return;
        }

        URL url = Thread.currentThread().getContextClassLoader()
                .getResource("matchinput/GoodMatchInput.csv");
        Assert.assertNotNull(url, "Cannot find GoodMatchInput.csv");

        try {
            CSVParser parser = CSVParser.parse(new File(url.getFile()), Charset.forName("UTF-8"), CSVFormat.EXCEL);
            accountPool = new ArrayList<>();
            boolean firstLine = true;
            for (CSVRecord csvRecord : parser) {
                if (firstLine) {
                    numFields = csvRecord.size();
                    firstLine = false;
                    continue;
                }
                List<Object> record = new ArrayList<>();
                for (String field : csvRecord) {
                    record.add(field);
                }
                accountPool.add(record);
            }
            log.info("Loaded " + accountPool.size() + " accounts into account pool.");
        } catch (IOException e) {
            Assert.fail("Failed to load account pool from GoodMatchInput.csv", e);
        }

    }

}
