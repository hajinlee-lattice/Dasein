package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.period.PeriodBuilder;

public class TimeSeriesUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesUtils.class);
    private static final String TIMESERIES_FILENAME_PREFIX = "Period-";
    private static final String TIMESERIES_FILENAME_SUFFIX = "-data.avro";
    private static final int MAX_ATTEMPTS = 3;

    // ONLY for testing purpose -- for testing retry time-series distribution
    // with failed period
    // set((periodName, periodId))
    private static ImmutableSet<Pair<String, Integer>> injectedFailedPeriods;

    /**
     * ONLY for testing purpose -- for testing retry time-series distribution
     * with failed period
     *
     * @param failedPeriods:
     *            set((periodName, periodId))
     */
    public static void injectFailedPeriods(ImmutableSet<Pair<String, Integer>> failedPeriods) {
        injectedFailedPeriods = failedPeriods;
    }

    public static Set<Integer> collectPeriods(Configuration yarnConfiguration, String avroDir,
            String periodField) {

        avroDir = getPath(avroDir) + "/*.avro";
        log.info(String.format("Collect %s periods from %s", periodField, avroDir));
        Set<Integer> periodSet = new HashSet<>();
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                periodSet.add(period);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect periods", e);
        }

        return periodSet;
    }

    public static Map<String, Set<Integer>> collectPeriods(Configuration yarnConfiguration,
            String avroDir, String periodField, String periodNameField) {

        avroDir = getPath(avroDir) + "/*.avro";
        log.info(String.format("Collect %s periods for %s from %s", periodField, periodNameField,
                avroDir));
        Map<String, Set<Integer>> periods = new HashMap<>();
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                String periodName = record.get(periodNameField).toString();
                if (!periods.containsKey(periodName)) {
                    periods.put(periodName, new HashSet<>());
                }
                periods.get(periodName).add(period);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect periods", e);
        }

        return periods;
    }

    public static void cleanupPeriodData(Configuration yarnConfiguration, String avroDir,
            Set<Integer> periods) {
        cleanupPeriodData(yarnConfiguration, avroDir, periods, false);
    }

    public static Long cleanupPeriodData(Configuration yarnConfiguration, String avroDir,
            Set<Integer> periods, boolean countRows) {
        avroDir = getPath(avroDir);
        log.info("Clean periods from " + avroDir);
        Long rowCounts = 0L;
        try {
            if (!HdfsUtils.isDirectory(yarnConfiguration, avroDir)) {
                return rowCounts;
            }
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir,
                    ".*.avro$");
            if (CollectionUtils.isEmpty(avroFiles)) {
                return rowCounts;
            }
            for (String fileName : avroFiles) {
                try {
                    if (periods == null) {
                        if (countRows) {
                            try {
                                rowCounts += AvroUtils.count(yarnConfiguration, fileName);
                            } catch (Exception e) {
                                log.error("Failed to count the rows for file: " + fileName);
                            }
                        }
                        HdfsUtils.rmdir(yarnConfiguration, fileName);
                    } else {
                        String[] dirs = fileName.split("/");
                        String avroName = dirs[dirs.length - 1];
                        Integer period = getPeriodFromFileName(avroName);
                        if ((period != null) && periods.contains(period)) {
                            if (countRows) {
                                try {
                                    rowCounts += AvroUtils.count(yarnConfiguration, fileName);
                                } catch (Exception e) {
                                    log.error("Failed to count the rows for file: " + fileName);
                                }
                            }
                            HdfsUtils.rmdir(yarnConfiguration, fileName);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to clean period data for file " + fileName, e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to clean period data", e);
        }
        return rowCounts;
    }

    public static void cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir) {
        log.info("Clean all periods from " + avroDir);
        cleanupPeriodData(yarnConfiguration, avroDir, null);
    }

    static String getPath(String avroDir) {
        return ProductUtils.getPath(avroDir);
    }

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir,
            String avroDir, Set<Integer> periods) {
        collectPeriodData(yarnConfiguration, targetDir, avroDir, periods, null);

    }

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir,
            String avroDir, Set<Integer> periods, String earliestTransaction) {
        avroDir = getPath(avroDir);
        targetDir = getPath(targetDir);
        log.info("Collect period data from " + avroDir + " to " + targetDir);

        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir,
                    ".*.avro$");
            for (String fileName : avroFiles) {
                String[] dirs = fileName.split("/");
                String avroName = dirs[dirs.length - 1];
                Integer period = getPeriodFromFileName(avroName);
                if (period == null) {
                    continue;
                }
                if (periods.contains(period)) {
                    log.info(String.format("Collect period data for period %d from file %s", period,
                            avroName));
                    HdfsUtils.copyFiles(yarnConfiguration, fileName, targetDir);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect period data", e);
        }
    }

    // periods: PeriodName -> PeriodIds
    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir,
            String avroDir, Map<String, Set<Integer>> periods,
            List<PeriodStrategy> periodStrategies, String earliestTransaction) {
        Map<String, PeriodBuilder> periodBuilders = new HashMap<>(); // PeriodName
                                                                     // ->
                                                                     // PeriodBuilder
        avroDir = getPath(avroDir);
        targetDir = getPath(targetDir);
        log.info("Collect period data from " + avroDir + " to " + targetDir);

        for (PeriodStrategy periodStrategy : periodStrategies) {
            PeriodBuilder periodBuilder = getPeriodBuilder(periodStrategy, earliestTransaction);
            periodBuilders.put(periodStrategy.getName(), periodBuilder);
        }

        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir,
                    ".*.avro$");
            Set<String> toCopy = new HashSet<>();
            for (String fileName : avroFiles) {
                String[] dirs = fileName.split("/");
                String avroName = dirs[dirs.length - 1];
                Integer dayPeriod = getPeriodFromFileName(avroName);
                if (dayPeriod == null) {
                    continue;
                }

                for (Map.Entry<String, PeriodBuilder> ent : periodBuilders.entrySet()) {
                    String periodName = ent.getKey();
                    PeriodBuilder periodBuilder = ent.getValue();
                    String periodDate = DateTimeUtils.dayPeriodToDate(dayPeriod);
                    Integer period = periodBuilder.toPeriodId(periodDate);
                    if (periods.get(periodName).contains(period)) {
                        log.info(String.format(
                                "Collect daily period %d for period %s from file %s (Will skip re-collect same daily data for different period)",
                                period, periodName, avroName));
                        toCopy.add(fileName);
                    }
                }
            }
            for (String fileName : toCopy) {
                HdfsUtils.copyFiles(yarnConfiguration, fileName, targetDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect period data", e);
        }
    }

    /**
     * distribute period data for single period store (daily store) with retry
     * IMPORTANT: will cleanup impacted period partition in target dir before
     * start/re-start distributing
     *
     * @param yarnConfiguration
     * @param inputDir
     * @param targetDir
     * @param periods
     * @param periodField
     */
    public static void distributePeriodDataWithRetry(Configuration yarnConfiguration, String inputDir, String targetDir,
            Set<Integer> periods, String periodField) {
        Set<Integer> periodsRemained = new HashSet<>(periods);
        // Only retry for non-LedpException
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_ATTEMPTS, Collections.singletonList(Exception.class),
                Collections.singletonList(LedpException.class));
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("Attempt #{} to distribute daily store", ctx.getRetryCount() + 1);
                // only for testing retry purpose: in retry phase, cleanup
                // injected failed period to let failed period pass
                injectedFailedPeriods = null;
            }
            // cleanup impacted periods in target dir
            TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, targetDir, periodsRemained);
            // distribute to target dir
            Set<Integer> failedPeriods = TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, targetDir,
                    periodsRemained, periodField, ctx.getRetryCount() + 1 == MAX_ATTEMPTS);
            periodsRemained.clear();
            if (CollectionUtils.isNotEmpty(failedPeriods)) {
                periodsRemained.addAll(failedPeriods);
            }
            if (ctx.getRetryCount() + 1 < MAX_ATTEMPTS && CollectionUtils.isNotEmpty(periodsRemained)) {
                throw new RuntimeException("Has periods failed to distribute");
            }
            return true;
        });
    }

    /**
     * distribute period data for single period store (daily store) without
     * retry
     *
     * @param yarnConfiguration
     * @param inputDir
     * @param targetDir
     * @param periods
     * @param periodField
     * @param ignoreFailure:
     *            whether to ignore intermittent HDFS write failure
     * @return failed periodIds -- return null if there is no failed period
     */
    public static Set<Integer> distributePeriodData(Configuration yarnConfiguration, String inputDir,
            String targetDir, Set<Integer> periods, String periodField, boolean ignoreFailure) {
        verifySchemaCompatibility(yarnConfiguration, inputDir, targetDir);
        inputDir = getPath(inputDir) + "/*.avro";
        targetDir = getPath(targetDir);
        log.info("Distribute period data from " + inputDir + " to " + targetDir);
        Map<Integer, String> periodFileMap = new HashMap<>();
        for (Integer period : periods) {
            periodFileMap.put(period, targetDir + "/" + getFileNameFromPeriod(period));
        }
        log.info("Distribute period IDs for daily period store: {}",
                String.join(",", periods.stream().map(String::valueOf).collect(Collectors.toList())));

        // Day -> set<periodIds>
        Map<String, Set<Integer>> failedPeriods = new HashMap<>();
        try {
            AvroFilesIterator iter = AvroUtils.iterateAvroFiles(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<Integer, List<GenericRecord>> dateRecordMap = new HashMap<>();
            List<Future<Pair<Boolean, Pair<String, Integer>>>> pendingWrites = new ArrayList<>();
            ExecutorService executor = ThreadPoolUtils
                    .getFixedSizeThreadPool("periodDataDistributor", 16);

            int totalRecords = 0;
            int pendingRecords = 0;
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                dateRecordMap.putIfAbsent(period, new ArrayList<>());
                dateRecordMap.get(period).add(record);
                totalRecords++;
                pendingRecords++;
                if (pendingRecords > 4 * 128 * 1024) {
                    log.info("Schedule " + pendingRecords + "records to write");
                    writeRecords(yarnConfiguration, executor, pendingWrites, schema, periodFileMap, dateRecordMap,
                            failedPeriods, ignoreFailure);
                    dateRecordMap.clear();
                    pendingRecords = 0;
                }
            }
            log.info("Schedule the remaining " + pendingRecords + " out of " + totalRecords
                    + " records to write");
            writeRecords(yarnConfiguration, executor, pendingWrites, schema, periodFileMap, dateRecordMap,
                    failedPeriods, ignoreFailure);
            syncWrites(pendingWrites, failedPeriods);
            Set<Integer> failedPeriodIds = failedPeriods.get(PeriodStrategy.Template.Day.name());
            if (CollectionUtils.isNotEmpty(failedPeriodIds)) {
                log.error("Period IDs failed to distribute: {}",
                        String.join(",", failedPeriodIds.stream().map(String::valueOf).collect(Collectors.toList())));
            }
            return failedPeriodIds;
        } catch (Exception e) {
            throw new RuntimeException("Failed to distribute to period store", e);
        }
    }

    /**
     * distribute period data for multi-period store with retry
     *
     * IMPORTANT: will cleanup impacted period partition in target dir before
     * start/re-start distributing
     *
     * @param yarnConfiguration
     * @param inputDir
     * @param targetDirs:
     *            (periodName -> target dir)
     * @param periods:
     *            (periodName -> set(periodIds))
     * @param periodField
     * @param periodNameField
     */
    public static void distributePeriodDataWithRetry(Configuration yarnConfiguration, String inputDir,
            Map<String, String> targetDirs, Map<String, Set<Integer>> periods, String periodField,
            String periodNameField) {
        Map<String, Set<Integer>> periodsRemained = new HashMap<>(periods);
        RetryTemplate retry = RetryUtils.getRetryTemplate(MAX_ATTEMPTS, Collections.singletonList(Exception.class),
                Collections.singletonList(LedpException.class));
        retry.execute(ctx -> {
            if (ctx.getRetryCount() > 0) {
                log.info("Attempt #{} to distribute period store", ctx.getRetryCount() + 1);
                // only for testing retry purpose: in retry phase, cleanup
                // injected failed period to let failed period pass
                injectedFailedPeriods = null;
            }
            // cleanup impacted periods in target dir
            for (String periodName : periodsRemained.keySet()) {
                TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, targetDirs.get(periodName),
                        periodsRemained.get(periodName));
            }
            // distribute to target dirs
            Map<String, Set<Integer>> failedPeriods = TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir,
                    targetDirs, periodsRemained, periodField, periodNameField, ctx.getRetryCount() + 1 == MAX_ATTEMPTS);
            periodsRemained.clear();
            if (MapUtils.isNotEmpty(failedPeriods)) {
                periodsRemained.putAll(failedPeriods);
            }
            if (ctx.getRetryCount() + 1 < MAX_ATTEMPTS && MapUtils.isNotEmpty(periodsRemained)) {
                throw new RuntimeException("Has periods failed to distribute");
            }
            return true;
        });
    }

    /**
     * distribute period data for multi-period store without retry
     *
     * @param yarnConfiguration
     * @param inputDir
     * @param targetDirs:
     *            (periodName -> target dir)
     * @param periods:
     *            (periodName -> set(periodIds))
     * @param periodField
     * @param periodNameField
     * @param ignoreFailure:
     *            whether to ignore intermittent HDFS write failure
     * @return failed periodIds: periodName -> set(periodIds)
     */
    public static Map<String, Set<Integer>> distributePeriodData(Configuration yarnConfiguration, String inputDir,
            Map<String, String> targetDirs, Map<String, Set<Integer>> periods, String periodField,
            String periodNameField, boolean ignoreFailure) {
        for (String targetDir : targetDirs.values()) {
            verifySchemaCompatibility(yarnConfiguration, inputDir, targetDir);
        }

        inputDir = getPath(inputDir) + "/*.avro";
        for (String periodName : targetDirs.keySet()) {
            String targetDir = getPath(targetDirs.get(periodName));
            log.info("Distribute period data from " + inputDir + " to " + targetDir);
            targetDirs.put(periodName, targetDir);
        }

        // periodName -> (periodId -> periodFile)
        Map<String, Map<Integer, String>> periodFileMap = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> ent : periods.entrySet()) {
            String periodName = ent.getKey();
            periodFileMap.put(periodName, new HashMap<>());
            for (Integer period : ent.getValue()) {
                periodFileMap.get(periodName).put(period,
                        targetDirs.get(periodName) + "/" + getFileNameFromPeriod(period));
            }
            log.info("Distribute period IDs for {} period store: {}", periodName,
                    String.join(",", ent.getValue().stream().map(String::valueOf).collect(Collectors.toList())));
        }

        Map<String, Set<Integer>> failedPeriods = new HashMap<>();
        try {
            AvroFilesIterator iter = AvroUtils.iterateAvroFiles(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<String, Map<Integer, List<GenericRecord>>> dateRecordMap = new HashMap<>();
            List<Future<Pair<Boolean, Pair<String, Integer>>>> pendingWrites = new ArrayList<>();
            ExecutorService executor = ThreadPoolUtils
                    .getFixedSizeThreadPool("periodDataDistributor", 16);

            int totalRecords = 0;
            int pendingRecords = 0;
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                String periodName = record.get(periodNameField).toString();
                dateRecordMap.putIfAbsent(periodName, new HashMap<>());
                dateRecordMap.get(periodName).putIfAbsent(period, new ArrayList<>());
                dateRecordMap.get(periodName).get(period).add(record);
                totalRecords++;
                pendingRecords++;
                if (pendingRecords > 2 * 128 * 1024) {
                    log.info("Schedule " + pendingRecords + "records to write");
                    writeRecordsMultiPeriod(yarnConfiguration, executor,
                            pendingWrites, schema, periodFileMap, dateRecordMap, failedPeriods, ignoreFailure);
                    pendingRecords = 0;
                    dateRecordMap.clear();
                }
            }
            log.info("Schedule the remaining " + pendingRecords + " out of " + totalRecords
                    + " records to write");
            writeRecordsMultiPeriod(yarnConfiguration, executor, pendingWrites, schema,
                    periodFileMap, dateRecordMap, failedPeriods, ignoreFailure);
            syncWrites(pendingWrites, failedPeriods);
            if (MapUtils.isNotEmpty(failedPeriods)) {
                for (Map.Entry<String, Set<Integer>> period : failedPeriods.entrySet()) {
                    if (CollectionUtils.isNotEmpty(period.getValue())) {
                        log.error("Period IDs failed to distribute to {} period store: {}", period.getKey(),
                                String.join(",",
                                        period.getValue().stream().map(String::valueOf).collect(Collectors.toList())));
                    }
                }
            }
            return failedPeriods;
        } catch (Exception e) {
            throw new RuntimeException("Failed to distribute to period store", e);
        }
    }

    /**
     * for single period store
     *
     * @param yarnConfiguration
     * @param executor
     * @param pendingWrites:
     *            future(success, (periodName, periodId))
     * @param schema
     * @param periodFileMap:
     *            (periodId -> periodFile)
     * @param dateRecordMap:
     *            periodId -> [records]
     * @param failedPeriods:
     *            (periodName -> set(periodId))
     * @param ignoreFailure:
     *            whether to ignore intermittent HDFS write failures
     */
    private static void writeRecords(Configuration yarnConfiguration,
            ExecutorService executor, List<Future<Pair<Boolean, Pair<String, Integer>>>> pendingWrites, Schema schema,
            Map<Integer, String> periodFileMap, Map<Integer, List<GenericRecord>> dateRecordMap,
            Map<String, Set<Integer>> failedPeriods, boolean ignoreFailure) {
        syncWrites(pendingWrites, failedPeriods);
        for (Integer period : dateRecordMap.keySet()) {
            List<GenericRecord> records = dateRecordMap.get(period);
            String fileName = periodFileMap.get(period);
            // Only distribute required periods. If cannot find target file, the
            // period is not in the required list
            if (fileName == null) {
                continue;
            }
            if ((records == null) || (records.size() == 0)) {
                continue;
            }
            // If HDFS write failure is not ignored and a period
            // already failed in previous write job, all the data
            // belonging to this period need to re-distribute anyway, thus no
            // need to continue distribute this period
            if (!ignoreFailure && failedPeriods.getOrDefault(PeriodStrategy.Template.Day.name(), new HashSet<>())
                    .contains(period)) {
                continue;
            }
            PeriodDataCallable callable = new PeriodDataCallable(yarnConfiguration, schema, fileName, records,
                    Pair.of(PeriodStrategy.Template.Day.name(), period));
            pendingWrites.add(executor.submit(callable));
        }
    }

    /**
     * for multi-period store
     *
     * @param yarnConfiguration
     * @param executor
     * @param pendingWrites:
     *            future(success, (periodName, periodId))
     * @param schema
     * @param periodFileMap:
     *            periodName -> (periodId -> periodFile)
     * @param dateRecordMap:
     *            periodName -> (periodId -> records)
     * @param failedPeriods:
     *            periodName -> set(periodId)
     * @param ignoreFailure:
     *            whether to ignore intermittent HDFS write failure
     */
    private static void writeRecordsMultiPeriod(Configuration yarnConfiguration,
            ExecutorService executor, List<Future<Pair<Boolean, Pair<String, Integer>>>> pendingWrites, Schema schema,
            Map<String, Map<Integer, String>> periodFileMap,
            Map<String, Map<Integer, List<GenericRecord>>> dateRecordMap, Map<String, Set<Integer>> failedPeriods,
            boolean ignoreFailure) {
        syncWrites(pendingWrites, failedPeriods);
        for (Map.Entry<String, Map<Integer, List<GenericRecord>>> ent : dateRecordMap.entrySet()) {
            String periodName = ent.getKey();
            for (Integer period : ent.getValue().keySet()) {
                List<GenericRecord> records = ent.getValue().get(period);
                String fileName = periodFileMap.get(periodName) == null ? null
                        : periodFileMap.get(periodName).get(period);
                // Only distribute required periods. If cannot find target file,
                // the period is not in the required list
                if (fileName == null) {
                    continue;
                }
                if ((records == null) || (records.size() == 0)) {
                    continue;
                }
                // If HDFS write failure is not ignored and a period
                // already failed in previous write job, all the data
                // belonging to this period need to re-distribute anyway, thus
                // no need to continue distribute this period
                if (!ignoreFailure && failedPeriods.getOrDefault(periodName, new HashSet<>()).contains(period)) {
                    continue;
                }
                PeriodDataCallable callable = new PeriodDataCallable(yarnConfiguration, schema, fileName, records,
                        Pair.of(periodName, period));
                pendingWrites.add(executor.submit(callable));
            }
        }
    }

    /**
     * @param pendingWrites:
     *            future(success, (periodName, periodId))
     * @param failedPeriods:
     *            periodName -> set(periodId)
     */
    private static void syncWrites(
            List<Future<Pair<Boolean, Pair<String, Integer>>>> pendingWrites, Map<String, Set<Integer>> failedPeriods) {
        for (Future<Pair<Boolean, Pair<String, Integer>>> pendingWrite : pendingWrites) {
            try {
                // (success, (periodName, periodId))
                Pair<Boolean, Pair<String, Integer>> res = pendingWrite.get();
                if (!Boolean.TRUE.equals(res.getLeft())) {
                    failedPeriods.putIfAbsent(res.getRight().getLeft(), new HashSet<>());
                    failedPeriods.get(res.getRight().getLeft()).add(res.getRight().getRight());
                }
            } catch (Exception e) {
                throw new RuntimeException("Error waiting for pending writes", e);
            }
        }
        pendingWrites.clear();
    }

    public static Pair<Integer, Integer> getMinMaxPeriod(Configuration yarnConfiguration, Table transactionTable) {
        try {
            String avroDir = transactionTable.getExtracts().get(0).getPath();
            avroDir = getPath(avroDir);
            log.info("Looking for earliest period table " + transactionTable.getName() + " path " + avroDir);
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
            Collections.sort(avroFiles);
            Integer minPeriod = getPeriodFromFileName(avroFiles.get(0));
            Integer maxPeriod = getPeriodFromFileName(avroFiles.get(avroFiles.size() - 1));
            log.info(String.format("Got min period %d from file %s and max period %d from file %s", minPeriod,
                    avroFiles.get(0), maxPeriod, avroFiles.get(avroFiles.size() - 1)));
            return Pair.of(minPeriod, maxPeriod);
        } catch (Exception e) {
            throw new RuntimeException("Failed to find earlies period", e);
        }
    }

    private static PeriodBuilder getPeriodBuilder(PeriodStrategy strategy, String minDateStr) {
        // strategy.setStartTimeStr(minDateStr);
        PeriodBuilder periodBuilder = PeriodBuilderFactory.build(strategy);
        return periodBuilder;
    }

    public static Integer getPeriodFromFileName(String fileName) {
        try {
            int beginIndex = fileName.indexOf(TIMESERIES_FILENAME_PREFIX) + TIMESERIES_FILENAME_PREFIX.length();
            int endIndex = fileName.indexOf(TIMESERIES_FILENAME_SUFFIX);
            return Integer.valueOf(fileName.substring(beginIndex, endIndex));
        } catch (Exception e) {
            throw new RuntimeException("Fail to get period id from file name " + fileName, e);
        }
    }

    static String getFileNameFromPeriod(Integer period) {
        return TIMESERIES_FILENAME_PREFIX + period + TIMESERIES_FILENAME_SUFFIX;
    }

    private static void verifySchemaCompatibility(Configuration yarnConfiguration, String sourceDir, String targetDir) {
        String sourceGlob = getPath(sourceDir) + "/*.avro";
        String targetGlob = getPath(targetDir) + "/*.avro";
        try {
            if (!AvroUtils.iterateAvroFiles(yarnConfiguration, targetGlob).hasNext()) {
                return;
            }
        } catch (Exception e) {
            log.warn("Failed to check non-emptiness of target glob " + targetGlob);
            return;
        }
        Schema sourceSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, sourceGlob);
        Schema targetSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, targetGlob);
        if (!areCompatibleSchemas(sourceSchema, targetSchema)) {
            throw new IllegalStateException(String.format(
                    "Source schema and target schema are incompatible:\nSource Schema:%s\nTarget Schema:%s\n", //
                    sourceSchema.toString(true), targetSchema.toString(true)));
        } else {
            log.info(String.format("Source Schema:%s\nTarget Schema:%s\n", sourceSchema.toString(true),
                    targetSchema.toString(true)));
        }
    }

    private static boolean areCompatibleSchemas(Schema schema1, Schema schema2) {
        List<String> cols1 = schema1.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        List<String> cols2 = schema2.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        if (cols1.size() != cols2.size()) {
            return false;
        }
        int size = cols1.size();
        return Arrays.equals(cols1.toArray(new String[size]), cols2.toArray(new String[size]));
    }

    static class PeriodDataCallable implements Callable<Pair<Boolean, Pair<String, Integer>>> {
        private Configuration yarnConfiguration;
        private Schema schema;
        private String fileName;
        private List<GenericRecord> records;
        // (periodName, periodId)
        private Pair<String, Integer> period;

        PeriodDataCallable(Configuration yarnConfiguration, Schema schema, String fileName,
                List<GenericRecord> records, Pair<String, Integer> period) {
            this.yarnConfiguration = yarnConfiguration;
            this.schema = schema;
            this.fileName = fileName;
            this.records = records;
            this.period = period;
        }

        @Override
        public Pair<Boolean, Pair<String, Integer>> call() throws Exception {
            try {
                // fail the write job only for testing purpose
                if (injectedFailedPeriods != null && injectedFailedPeriods.contains(period)) {
                    return Pair.of(Boolean.FALSE, period);
                }
                if (!HdfsUtils.fileExists(yarnConfiguration, fileName)) {
                    AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fileName, records);
                } else {
                    AvroUtils.appendToHdfsFile(yarnConfiguration, fileName, records);
                }
            } catch (Exception e) {
                log.error(String.format("Failed to distribute period data %s-%d to %s", period.getLeft(),
                        period.getRight(), fileName), e);
                return Pair.of(Boolean.FALSE, period);
            }
            return Pair.of(Boolean.TRUE, period);
        }
    }
}
