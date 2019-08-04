package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class TimeSeriesDistributer {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesDistributer.class);

    // To fake period name for single period data store without period name
    public static final String DUMMY_PERIOD = "DUMMY_PERIOD";
    private static final int BATCH_SIZE = 1000;
    private static final int DISTRIBUTERS = 16;
    private static final int BUFFER_THRESHOLD = 2 * 128 * 1024;
    private static final long WRITER_TIMEOUT_SEC = 3600;

    private Configuration yarnConfig;
    private String inputDir;
    // PeriodName -> TargetDir
    private Map<String, String> targetDirs;
    // PeriodName -> [PeriodId]
    private Map<String, Set<Integer>> periods;
    // (PeriodName, PeriodId) -> Target File
    private Map<Pair<String, Integer>, String> targetFiles;
    // PeriodId field name in input avro
    private String periodField;
    // PeriodName field name in input avro
    private String periodNameField;

    // iterator of records from avro file
    private AvroFilesIterator avroIter;
    // avro schema
    private Schema schema;
    // Read buffer (records read from HDFS, but pending for writing):
    // (PeriodName, PeriodId) -> [Records]
    private Map<Pair<String, Integer>, List<GenericRecord>> readBuffer;
    // (PeriodName, PeriodId) -> (Job start time, Job size -- number of records)
    Map<Pair<String, Integer>, Pair<Long, Integer>> writers;
    ExecutorService executor;
    // ((PeriodName, PeriodId), success)
    List<Future<Pair<Pair<String, Integer>, Boolean>>> futures;

    TimeSeriesDistributer() {
    }

    /**
     * Initialization
     */
    private void setup() {
        this.avroIter = AvroUtils.iterateAvroFiles(yarnConfig, inputDir);
        this.schema = AvroUtils.getSchemaFromGlob(yarnConfig, inputDir);
        this.targetFiles = getPeriodFileMap(targetDirs, periods);
        this.readBuffer = new HashMap<>();
        this.writers = new HashMap<>();
        this.executor = ThreadPoolUtils.getFixedSizeThreadPool("periodDataDistributor", DISTRIBUTERS);
        this.futures = new LinkedList<>();
    }

    /**
     * Distribute period data to target directory based on different
     * (PeriodName+PeriodId)
     */
    public void distributePeriodData() {
        // targetDirs: PeriodName -> TargetDir
        for (String targetDir : targetDirs.values()) {
            verifySchemaCompatibility(inputDir, targetDir);
        }
        setup();

        try {
            while (avroIter.hasNext()) {
                consumeBatchWriteFutures();
                if (getTotalBufferSize() > BUFFER_THRESHOLD) {
                    batchWritePeriodData(null);
                    try {
                        Thread.sleep(1_000L);
                    } catch (InterruptedException e) {
                    }
                    continue;
                }
                batchReadPeriodData();
                batchWritePeriodData(BATCH_SIZE);
            }
            // drain read buffer
            while (getReadBufferSize() != 0) {
                consumeBatchWriteFutures();
                batchWritePeriodData(null);
                try {
                    Thread.sleep(1_000L);
                } catch (InterruptedException e) {
                }
            }
            // drain write buffer
            while (getWriteBufferSize() != 0) {
                syncConsumeBatchWriteFutures();
            }
        } catch (Exception e) {
            throw e;
        } finally {
            shutdownBatchWriteExecutor();
        }
    }

    /**
     * Batch read records from HDFS avro file
     */
    private void batchReadPeriodData() {
        int cnt = 0;
        try (PerformanceTimer timer = new PerformanceTimer(String.format("Fetch %d time-series records", cnt))) {
            timer.setThreshold(1000);

            while (cnt < BATCH_SIZE && avroIter.hasNext()) {
                GenericRecord record = avroIter.next();
                String periodName = periodNameField == null ? DUMMY_PERIOD
                        : (record.get(periodNameField) == null ? DUMMY_PERIOD : record.get(periodNameField).toString());
                if (periodNameField != null && periodName == null) {
                    throw new LedpException(LedpCode.LEDP_41001, new String[] { periodNameField, record.toString() });
                }
                Integer periodId = record.get(periodField) == null ? null : (Integer) record.get(periodField);
                if (periodId == null) {
                    throw new LedpException(LedpCode.LEDP_41002, new String[] { periodField, record.toString() });
                }
                Pair<String, Integer> period = Pair.of(periodName, periodId);
                // readBuffer: (PariodName, PeriodId) -> [records]
                if (!readBuffer.containsKey(period)) {
                    readBuffer.put(period, new ArrayList<>());
                }
                readBuffer.get(period).add(record);
                cnt++;
            }

        }
    }

    /**
     * Batch write records to HDFS
     *
     * @param sizeThreshold:
     *            If provided, only submit write job when records number exceeds
     *            threshold; Otherwise, submit directly even if there is only
     *            few records
     */
    private void batchWritePeriodData(Integer sizeThreshold) {
        // Pending write job number larger than thread pool size. No need to
        // submit more.
        if (futures.size() >= DISTRIBUTERS) {
            return;
        }

        // readBuffer: (PeriodName, PeriodId) -> [records]
        Iterator<Map.Entry<Pair<String, Integer>, List<GenericRecord>>> readIter = readBuffer.entrySet()
                .iterator();
        while (readIter.hasNext()) {
            Map.Entry<Pair<String, Integer>, List<GenericRecord>> periodMap = readIter.next();
            Pair<String, Integer> period = periodMap.getKey();
            List<GenericRecord> records = periodMap.getValue();
            // Only allow single thread writing to one period file (named by
            // PeriodName + PeriodId) as HDFS is multi-read but single-write
            if (writers.containsKey(period)) {
                continue;
            }
            if (sizeThreshold != null && records.size() < sizeThreshold) {
                continue;
            }
            // (PeriodName, PeriodId) -> (start time, size)
            writers.put(period, Pair.of(System.currentTimeMillis(), records.size()));
            // future: ((PeriodName, PeriodId), success)
            Future<Pair<Pair<String, Integer>, Boolean>> future = executor.submit(
                    getBatchWriteCallable(targetFiles.get(period), records, period));
            futures.add(future);
            readIter.remove();
            if (futures.size() >= DISTRIBUTERS) {
                return;
            }
        }
    }

    /**
     * @param targetFile:
     *            target file's full path
     * @param records:
     *            [records]
     * @param period:
     *            (PeriodName, PeriodId)
     * @return
     */
    private Callable<Pair<Pair<String, Integer>, Boolean>> getBatchWriteCallable(String targetFile,
            List<GenericRecord> records, Pair<String, Integer> period) {
        Callable<Pair<Pair<String, Integer>, Boolean>> task = new Callable<Pair<Pair<String, Integer>, Boolean>>() {
            @Override
            public Pair<Pair<String, Integer>, Boolean> call() {
                try {
                    try (PerformanceTimer timer = new PerformanceTimer(
                            String.format("Distribute %d time-series records", records.size()))) {
                        timer.setThreshold(1000);

                        if (!HdfsUtils.fileExists(yarnConfig, targetFile)) {
                            AvroUtils.writeToHdfsFile(yarnConfig, schema, targetFile, records);
                        } else {
                            AvroUtils.appendToHdfsFile(yarnConfig, targetFile, records);
                        }
                    }
                } catch (Exception e) {
                    log.error(String.format("Fail to write %d records to file.", records.size(), targetFile), e);
                    return Pair.of(period, Boolean.FALSE);
                }
                return Pair.of(period, Boolean.TRUE);
            }
        };
        return task;
    }

    /**
     * Check whether batch write job is finished or not. If finished, remove it
     * from writeBuffer and futures. If timeout, fail the write job.
     */
    private void consumeBatchWriteFutures() {
        // future: ((PeriodName, PeriodId), success)
        Iterator<Future<Pair<Pair<String, Integer>, Boolean>>> iter = futures.iterator();
        while (iter.hasNext()) {
            Future<Pair<Pair<String, Integer>, Boolean>> future = iter.next();
            if (future.isDone()) {
                Pair<Pair<String, Integer>, Boolean> res;
                try {
                    // When future is done, get() should not be blocked. Add
                    // 1-min timeout as safeguard in case it got hanging under
                    // some weird cases
                    res = future.get(1, TimeUnit.MINUTES);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    throw new RuntimeException("Batch write of period data failed");
                }
                iter.remove();
                // writers: (PeriodName, PeriodId) -> (start time, size)
                writers.remove(res.getLeft());
                if (!Boolean.TRUE.equals(res.getRight())) {
                    throw new RuntimeException("Batch write of period data failed");
                }
            }
        }
        checkWriterTimeout();
    }

    /**
     * Timeout write job if running for too long
     */
    private void checkWriterTimeout() {
        long current = System.currentTimeMillis();
        // writers: (PeriodName, PeriodId) -> (start time, size)
        writers.forEach((period, status) -> {
            if (current - status.getLeft() > WRITER_TIMEOUT_SEC) {
                throw new RuntimeException(
                        String.format("Batch write for period %s-%d got timeout", period.getLeft(), period.getRight()));
            }
        });
    }

    /**
     * Check batch write job is finished or not. If finished, remove it from
     * writeBuffer and futures; If not finished, wait for it to finish until
     * timeout
     */
    private void syncConsumeBatchWriteFutures() {
        // future: ((PeriodName, PeriodId), success)
        Iterator<Future<Pair<Pair<String, Integer>, Boolean>>> futureIter = futures.iterator();
        log.info("Last {} distribute jobs to finish.", futures.size());
        while (futureIter.hasNext()) {
            Future<Pair<Pair<String, Integer>, Boolean>> future = futureIter.next();
            Pair<Pair<String, Integer>, Boolean> res;
            try {
                res = future.get(1, TimeUnit.HOURS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException("Batch write future got timeout");
            }
            futureIter.remove();
            // writers: (PeriodName, PeriodId) -> (start time, size)
            writers.remove(res.getLeft());
            if (!Boolean.TRUE.equals(res.getRight())) {
                throw new RuntimeException("Batch write of period data failed");
            }
        }
    }

    /**
     * Legacy code moved from TimeSeridsUtils to check whether avros in
     * sourceDir and targetDir have same schema
     *
     * @param sourceDir
     * @param targetDir
     */
    private void verifySchemaCompatibility(String sourceDir, String targetDir) {
        String sourceGlob = TimeSeriesUtils.getPath(sourceDir) + "/*.avro";
        String targetGlob = TimeSeriesUtils.getPath(targetDir) + "/*.avro";
        try {
            if (!AvroUtils.iterateAvroFiles(yarnConfig, targetGlob).hasNext()) {
                return;
            }
        } catch (Exception e) {
            log.warn("Failed to check non-emptiness of target glob " + targetGlob);
            return;
        }
        Schema sourceSchema = AvroUtils.getSchemaFromGlob(yarnConfig, sourceGlob);
        Schema targetSchema = AvroUtils.getSchemaFromGlob(yarnConfig, targetGlob);
        if (!isSchemaCompatible(sourceSchema, targetSchema)) {
            throw new IllegalStateException(String.format(
                    "Source schema and target schema are incompatible:\nSource Schema:%s\nTarget Schema:%s\n", //
                    sourceSchema.toString(true), targetSchema.toString(true)));
        } else {
            log.info(String.format("Source Schema:%s\nTarget Schema:%s\n", sourceSchema.toString(true),
                    targetSchema.toString(true)));
        }
    }

    /**
     * Legacy code moved from TimeSeridsUtils to check compatible schema
     *
     * TODO: Better add type check too
     *
     * @param schema1
     * @param schema2
     * @return
     */
    private boolean isSchemaCompatible(Schema schema1, Schema schema2) {
        List<String> cols1 = schema1.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        List<String> cols2 = schema2.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        if (cols1.size() != cols2.size()) {
            return false;
        }
        int size = cols1.size();
        return Arrays.equals(cols1.toArray(new String[size]), cols2.toArray(new String[size]));
    }

    private static String standardizeDistributeInputDir(String inputDir) {
        inputDir = TimeSeriesUtils.getPath(inputDir) + "/*.avro";
        log.info("Distribute period data from {}", inputDir);
        return inputDir;
    }

    private static Map<String, String> standardizeDistributeTgtDirs(Map<String, String> targetDirs) {
        for (String periodName : targetDirs.keySet()) {
            String targetDir = TimeSeriesUtils.getPath(targetDirs.get(periodName));
            log.info("Distribute period data from to {}", targetDir);
            targetDirs.put(periodName, targetDir);
        }
        return targetDirs;
    }

    /**
     * @param targetDirs:
     *            PeriodName -> Target directory of data to distribute to
     * @param periods:
     *            PeriodName -> Set of PeriodId
     * @return <(PeriodName,PeriodId) -> PeriodFileName>
     */
    private Map<Pair<String, Integer>, String> getPeriodFileMap(Map<String, String> targetDirs,
            Map<String, Set<Integer>> periods) {
        Map<Pair<String, Integer>, String> periodFileMap = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> ent : periods.entrySet()) {
            String periodName = ent.getKey();
            for (Integer period : ent.getValue()) {
                periodFileMap.put(Pair.of(periodName, period),
                        targetDirs.get(periodName) + "/" + TimeSeriesUtils.getFileNameFromPeriod(period));
                log.debug("Add period " + period + " File " + targetDirs.get(periodName) + "/"
                        + TimeSeriesUtils.getFileNameFromPeriod(period));
            }
        }
        return periodFileMap;
    }

    /**
     * Shut down batch write executor
     */
    private void shutdownBatchWriteExecutor() {
        futures.forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    // Throw LedpException to avoid retry -- If somehow some
                    // background thread is still writing to HDFS, it might mess
                    // up data in retried job.
                    throw new LedpException(LedpCode.LEDP_41003);
                }
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            throw new LedpException(LedpCode.LEDP_41003);
        }
    }

    /******************
     * Getter & Setter
     ******************/

    void setYarnConfig(Configuration yarnConfig) {
        this.yarnConfig = yarnConfig;
    }

    void setInputDir(String inputDir) {
        this.inputDir = standardizeDistributeInputDir(inputDir);
    }

    void setTargetDirs(Map<String, String> targetDirs) {
        this.targetDirs = standardizeDistributeTgtDirs(targetDirs);
    }

    void setPeriods(Map<String, Set<Integer>> periods) {
        this.periods = periods;
    }

    void setPeriodField(String periodField) {
        this.periodField = periodField;
    }

    void setPeriodNameField(String periodNameField) {
        this.periodNameField = periodNameField;
    }

    private int getTotalBufferSize() {
        return getReadBufferSize() + getWriteBufferSize();
    }

    private int getReadBufferSize() {
        return readBuffer.values().stream().collect(Collectors.summingInt(records -> records.size()));
    }

    private int getWriteBufferSize() {
        return writers.values().stream().collect(Collectors.summingInt(status -> status.getRight()));
    }


    public static class DistributerBuilder {
        private TimeSeriesDistributer distributer;

        public DistributerBuilder() {
            this.distributer = new TimeSeriesDistributer();
        }

        public DistributerBuilder yarnConfig(Configuration yarnConfig) {
            this.distributer.setYarnConfig(yarnConfig);
            return this;
        }

        public DistributerBuilder inputDir(String inputDir) {
            this.distributer.setInputDir(inputDir);
            return this;
        }

        public DistributerBuilder targetDirs(Map<String, String> targetDirs) {
            this.distributer.setTargetDirs(targetDirs);
            return this;
        }

        public DistributerBuilder periods(Map<String, Set<Integer>> periods) {
            this.distributer.setPeriods(periods);
            return this;
        }

        public DistributerBuilder periodField(String periodField) {
            this.distributer.setPeriodField(periodField);
            return this;
        }

        public DistributerBuilder periodNameField(String periodNameField) {
            this.distributer.setPeriodNameField(periodNameField);
            return this;
        }

        public TimeSeriesDistributer build() {
            return this.distributer;
        }
    }
}

