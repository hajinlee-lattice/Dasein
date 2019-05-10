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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.cdl.PeriodBuilderFactory;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.period.PeriodBuilder;

public class TimeSeriesUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesUtils.class);
    private static final String TIMESERIES_FILENAME_PREFIX = "Period-";
    private static final String TIMESERIES_FILENAME_SUFFIX = "-data.avro";

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

    public static void cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir,
            Set<Integer> periods) {
        cleanupPeriodData(yarnConfiguration, avroDir, periods, false);
    }

    public static Long cleanupPeriodData(Configuration yarnConfiguration, String avroDir,
            Set<Integer> periods, boolean countRows) {
        avroDir = getPath(avroDir);
        log.info("Clean periods from " + avroDir);
        Long rowCounts = 0L;
        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir,
                    ".*.avro$");
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
                    log.error("Failed to clean period data for file " + fileName, e);
                }
            }
        } catch (Exception e) {
            log.error("Failed to clean period data", e);
        }
        return rowCounts;
    }

    public static void cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir) {
        log.info("Clean all periods from " + avroDir);
        cleanupPeriodData(yarnConfiguration, avroDir, null);
    }

    private static String getPath(String avroDir) {
        log.info("Get avro path input " + avroDir);
        if (!avroDir.endsWith(".avro")) {
            return avroDir;
        } else {
            String[] dirs = avroDir.trim().split("/");
            avroDir = "";
            for (int i = 0; i < (dirs.length - 1); i++) {
                log.info("Get avro path dir " + dirs[i]);
                if (!dirs[i].isEmpty()) {
                    avroDir = avroDir + "/" + dirs[i];
                }
            }
        }
        log.info("Get avro path output " + avroDir);
        return avroDir;
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
            log.error("Failed to collect period data", e);
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
            log.error("Failed to collect period data", e);
        }
    }

    public static boolean distributePeriodData(Configuration yarnConfiguration, String inputDir,
            String targetDir, Set<Integer> periods, String periodField) {
        verifySchemaCompatibility(yarnConfiguration, inputDir, targetDir);
        inputDir = getPath(inputDir) + "/*.avro";
        targetDir = getPath(targetDir);
        log.info("Distribute period data from " + inputDir + " to " + targetDir);
        Map<Integer, String> periodFileMap = new HashMap<>();
        for (Integer period : periods) {
            periodFileMap.put(period, targetDir + "/" + getFileNameFromPeriod(period));
            log.info("Add period " + period + " File " + targetDir + "/"
                    + getFileNameFromPeriod(period));
        }
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<Integer, List<GenericRecord>> dateRecordMap = new HashMap<>();
            List<Future<Boolean>> pendingWrites = new ArrayList<>();
            ExecutorService executor = ThreadPoolUtils
                    .getFixedSizeThreadPool("periodDataDistributor", 16);

            int totalRecords = 0;
            int pendingRecords = 0;
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                if (!dateRecordMap.containsKey(period)) {
                    dateRecordMap.put(period, new ArrayList<>());
                }
                dateRecordMap.get(period).add(record);
                totalRecords++;
                pendingRecords++;
                if (pendingRecords > 4 * 128 * 1024) {
                    log.info("Schedule " + pendingRecords + "records to write");
                    dateRecordMap = writeRecords(yarnConfiguration, executor, pendingWrites, schema,
                            periodFileMap, dateRecordMap);
                    pendingRecords = 0;
                }
            }
            log.info("Schedule the remaining " + pendingRecords + " out of " + totalRecords
                    + " records to write");
            writeRecords(yarnConfiguration, executor, pendingWrites, schema, periodFileMap,
                    dateRecordMap);
            syncWrites(pendingWrites);
            return true;
        } catch (Exception e) {
            log.error("Failed to distribute to period store", e);
            return false;
        }
    }

    // MultiPeriod mode
    // targetDirs: PeriodName -> TargetDir
    // periods: PeriodName -> Periods
    public static boolean distributePeriodData(Configuration yarnConfiguration, String inputDir,
            Map<String, String> targetDirs, Map<String, Set<Integer>> periods, String periodField,
            String periodNameField) {
        for (String targetDir : targetDirs.values()) {
            verifySchemaCompatibility(yarnConfiguration, inputDir, targetDir);
        }

        inputDir = getPath(inputDir) + "/*.avro";
        for (String periodName : targetDirs.keySet()) {
            String targetDir = getPath(targetDirs.get(periodName));
            log.info("Distribute period data from " + inputDir + " to " + targetDir);
            targetDirs.put(periodName, targetDir);
        }

        // PeriodName -> (PeriodId -> PeriodFile)
        Map<String, Map<Integer, String>> periodFileMap = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> ent : periods.entrySet()) {
            String periodName = ent.getKey();
            periodFileMap.put(periodName, new HashMap<>());
            for (Integer period : ent.getValue()) {
                periodFileMap.get(periodName).put(period,
                        targetDirs.get(periodName) + "/" + getFileNameFromPeriod(period));
                log.info("Add period " + period + " File" + targetDirs.get(periodName) + "/"
                        + getFileNameFromPeriod(period));
            }
        }

        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<String, Map<Integer, List<GenericRecord>>> dateRecordMap = new HashMap<>();
            List<Future<Boolean>> pendingWrites = new ArrayList<>();
            ExecutorService executor = ThreadPoolUtils
                    .getFixedSizeThreadPool("periodDataDistributor", 16);

            int totalRecords = 0;
            int pendingRecords = 0;
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                String periodName = record.get(periodNameField).toString();
                if (!dateRecordMap.containsKey(periodName)) {
                    dateRecordMap.put(periodName, new HashMap<>());
                }
                if (!dateRecordMap.get(periodName).containsKey(period)) {
                    dateRecordMap.get(periodName).put(period, new ArrayList<>());
                }
                dateRecordMap.get(periodName).get(period).add(record);
                totalRecords++;
                pendingRecords++;
                if (pendingRecords > 2 * 128 * 1024) {
                    log.info("Schedule " + pendingRecords + "records to write");
                    dateRecordMap = writeRecordsMultiPeriod(yarnConfiguration, executor,
                            pendingWrites, schema, periodFileMap, dateRecordMap);
                    pendingRecords = 0;
                }
            }
            log.info("Schedule the remaining " + pendingRecords + " out of " + totalRecords
                    + " records to write");
            writeRecordsMultiPeriod(yarnConfiguration, executor, pendingWrites, schema,
                    periodFileMap, dateRecordMap);
            syncWrites(pendingWrites);
            return true;
        } catch (Exception e) {
            log.error("Failed to distribute to period store", e);
            return false;
        }
    }

    private static Map<Integer, List<GenericRecord>> writeRecords(Configuration yarnConfiguration,
            ExecutorService executor, List<Future<Boolean>> pendingWrites, Schema schema,
            Map<Integer, String> periodFileMap, Map<Integer, List<GenericRecord>> dateRecordMap) {
        syncWrites(pendingWrites);
        for (Integer period : dateRecordMap.keySet()) {
            List<GenericRecord> records = dateRecordMap.get(period);
            String fileName = periodFileMap.get(period);
            if (fileName == null) {
                log.info("Failed to find file for " + period);
                continue;
            }
            if ((records == null) || (records.size() == 0)) {
                continue;
            }
            PeriodDataCallable callable = new PeriodDataCallable(yarnConfiguration, schema,
                    fileName, records);
            pendingWrites.add(executor.submit(callable));
        }

        return new HashMap<>();
    }

    // periodFileMap: periodName -> (periodId -> periodFile)
    // dateRecordMap: periodName -> (periodId -> records)
    private static Map<String, Map<Integer, List<GenericRecord>>> writeRecordsMultiPeriod(
            Configuration yarnConfiguration, ExecutorService executor,
            List<Future<Boolean>> pendingWrites, Schema schema,
            Map<String, Map<Integer, String>> periodFileMap,
            Map<String, Map<Integer, List<GenericRecord>>> dateRecordMap) {
        syncWrites(pendingWrites);
        for (Map.Entry<String, Map<Integer, List<GenericRecord>>> ent : dateRecordMap.entrySet()) {
            String periodName = ent.getKey();
            for (Integer period : ent.getValue().keySet()) {
                List<GenericRecord> records = ent.getValue().get(period);
                String fileName = periodFileMap.get(periodName).get(period);
                if (fileName == null) {
                    log.info("Failed to find file for " + period);
                    continue;
                }
                if ((records == null) || (records.size() == 0)) {
                    continue;
                }
                PeriodDataCallable callable = new PeriodDataCallable(yarnConfiguration, schema,
                        fileName, records);
                pendingWrites.add(executor.submit(callable));
            }
        }

        return new HashMap<>();
    }

    private static void syncWrites(List<Future<Boolean>> pendingWrites) {
        for (Future<Boolean> pendingWrite : pendingWrites) {
            try {
                pendingWrite.get();
            } catch (Exception e) {
                log.error("Error waiting for pending writes", e);
                continue;
            }
        }
        pendingWrites.clear();
    }

    public static Pair<Integer, Integer> getMinMaxPeriod(Configuration yarnConfiguration,
            Table transactionTable) {
        try {
            String avroDir = transactionTable.getExtracts().get(0).getPath();
            avroDir = getPath(avroDir);
            log.info("Looking for earliest period table " + transactionTable.getName() + " path "
                    + avroDir);
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir,
                    ".*.avro$");
            Collections.sort(avroFiles);
            Integer minPeriod = getPeriodFromFileName(avroFiles.get(0));
            Integer maxPeriod = getPeriodFromFileName(avroFiles.get(avroFiles.size() - 1));
            log.info(String.format("Got min period %d from file %s and max period %d from file %s",
                    minPeriod, avroFiles.get(0), maxPeriod, avroFiles.get(avroFiles.size() - 1)));
            return Pair.of(minPeriod, maxPeriod);
        } catch (Exception e) {
            log.error("Failed to find earlies period", e);
            return null;
        }
    }

    private static PeriodBuilder getPeriodBuilder(PeriodStrategy strategy, String minDateStr) {
        // strategy.setStartTimeStr(minDateStr);
        PeriodBuilder periodBuilder = PeriodBuilderFactory.build(strategy);
        return periodBuilder;
    }

    public static Integer getPeriodFromFileName(String fileName) {
        try {
            int beginIndex = fileName.indexOf(TIMESERIES_FILENAME_PREFIX)
                    + TIMESERIES_FILENAME_PREFIX.length();
            int endIndex = fileName.indexOf(TIMESERIES_FILENAME_SUFFIX);
            return Integer.valueOf(fileName.substring(beginIndex, endIndex));
        } catch (Exception e) {
            throw new RuntimeException("Fail to get period id from file name " + fileName, e);
        }
    }

    private static String getFileNameFromPeriod(Integer period) {
        return TIMESERIES_FILENAME_PREFIX + period + TIMESERIES_FILENAME_SUFFIX;
    }

    private static void verifySchemaCompatibility(Configuration yarnConfiguration, String sourceDir,
            String targetDir) {
        String sourceGlob = getPath(sourceDir) + "/*.avro";
        String targetGlob = getPath(targetDir) + "/*.avro";
        try {
            if (!AvroUtils.iterator(yarnConfiguration, targetGlob).hasNext()) {
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
            log.info(String.format("Source Schema:%s\nTarget Schema:%s\n",
                    sourceSchema.toString(true), targetSchema.toString(true)));
        }
    }

    private static boolean areCompatibleSchemas(Schema schema1, Schema schema2) {
        List<String> cols1 = schema1.getFields().stream().map(Schema.Field::name)
                .collect(Collectors.toList());
        List<String> cols2 = schema2.getFields().stream().map(Schema.Field::name)
                .collect(Collectors.toList());
        if (cols1.size() != cols2.size()) {
            return false;
        }
        int size = cols1.size();
        return Arrays.equals(cols1.toArray(new String[size]), cols2.toArray(new String[size]));
    }

    static class PeriodDataCallable implements Callable<Boolean> {
        private Configuration yarnConfiguration;
        private Schema schema;
        private String fileName;
        private List<GenericRecord> records;

        PeriodDataCallable(Configuration yarnConfiguration, Schema schema, String fileName,
                List<GenericRecord> records) {

            this.yarnConfiguration = yarnConfiguration;
            this.schema = schema;
            this.fileName = fileName;
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                if (!HdfsUtils.fileExists(yarnConfiguration, fileName)) {
                    AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fileName, records);
                } else {
                    AvroUtils.appendToHdfsFile(yarnConfiguration, fileName, records);
                }
            } catch (Exception e) {
                log.error("Failed to distribute period data to " + fileName, e);
            }
            return Boolean.TRUE;
        }
    }

}
