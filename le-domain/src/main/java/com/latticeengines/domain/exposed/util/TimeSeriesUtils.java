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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.period.CalendarMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class TimeSeriesUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesUtils.class);

    public static Set<Integer> collectPeriods(Configuration yarnConfiguration, String avroDir, String periodField) {

        avroDir = getPath(avroDir) + "/*.avro";
        log.info("Collect " + periodField + " periods from " + avroDir);
        Set<Integer> periodSet = new HashSet<Integer>();
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer) record.get(periodField);
                periodSet.add(period);
            }
        } catch (Exception e) {
            log.error("Failed to collect periods for period table", e);
        }

        return periodSet;
    }

    public static void cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir, Set<Integer> periods) {
        cleanupPeriodData(yarnConfiguration, avroDir, periods, false);
    }

    public static Long cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir, Set<Integer> periods,
            boolean countRows) {
        avroDir = getPath(avroDir);
        log.info("Clean periods from " + avroDir);
        Long rowCounts = 0L;
        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
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

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir, String avroDir,
            Set<Integer> periods) {
        collectPeriodData(yarnConfiguration, targetDir, avroDir, periods, null, null);

    }

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir, String avroDir,
            Set<Integer> periods, PeriodStrategy periodStrategy, String earliestTransaction) {
        PeriodBuilder periodBuilder = null;
        avroDir = getPath(avroDir);
        targetDir = getPath(targetDir);
        log.info("Collect period data from " + avroDir + " to " + targetDir);

        if (periodStrategy != null) {
            periodBuilder = getPeriodBuilder(periodStrategy, earliestTransaction);
        }

        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
            for (String fileName : avroFiles) {
                String[] dirs = fileName.split("/");
                String avroName = dirs[dirs.length - 1];
                Integer period = getPeriodFromFileName(avroName);
                log.info("Collect period data from file " + avroName + " period " + period);
                if (period == null) {
                    continue;
                }
                if (periodBuilder != null) {
                    period = periodBuilder.toPeriodId(DateTimeUtils.dayPeriodToDate(period));
                }
                if (periods.contains(period)) {
                    HdfsUtils.copyFiles(yarnConfiguration, fileName, targetDir);
                }
            }
        } catch (Exception e) {
            log.error("Failed to collect period data", e);
        }
    }

    public static boolean distributePeriodData(Configuration yarnConfiguration, String inputDir, String targetDir,
            Set<Integer> periods, String periodField) {
        verifySchemaCompatibility(yarnConfiguration, inputDir, targetDir);
        inputDir = getPath(inputDir) + "/*.avro";
        targetDir = getPath(targetDir);
        log.info("Distribute period data from " + inputDir + " to " + targetDir);
        Map<Integer, String> periodFileMap = new HashMap<Integer, String>();
        for (Integer period : periods) {
            periodFileMap.put(period, targetDir + "/" + getFileNameFromPeriod(period));
            log.info("Add period " + period + " File" + targetDir + "/" + getFileNameFromPeriod(period));
        }
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<Integer, List<GenericRecord>> dateRecordMap = new HashMap<Integer, List<GenericRecord>>();
            List<Future<Boolean>> pendingWrites = new ArrayList<Future<Boolean>>();
            ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool("periodDataDistributor", 16);

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
                if (pendingRecords > 128 * 1024) {
                    log.info("Schedule " + pendingRecords + "records to write");
                    dateRecordMap = writeRecords(yarnConfiguration, executor, pendingWrites, schema, periodFileMap, dateRecordMap);
                    pendingRecords = 0;
                }
            }

            log.info("Schedule the remaining " + pendingRecords + " out of " + totalRecords + " records to write");
            writeRecords(yarnConfiguration, executor, pendingWrites, schema, periodFileMap, dateRecordMap);
            syncWrites(pendingWrites);
            return true;
        } catch (Exception e) {
            log.error("Failed to distribute to period store", e);
            return false;
        }
    }

    private static Map<Integer, List<GenericRecord>> writeRecords(Configuration yarnConfiguration, ExecutorService executor,
                                                           List<Future<Boolean>> pendingWrites, Schema schema,
                                                           Map<Integer, String> periodFileMap, Map<Integer, List<GenericRecord>> dateRecordMap) {
        syncWrites(pendingWrites);
        for (Integer period : dateRecordMap.keySet()) {
           List<GenericRecord> records = dateRecordMap.get(period);
           String fileName = periodFileMap.get(period);
           if( fileName == null) {
               log.info("Failed to find file for " + period);
               continue;
           }
           if ((records == null) || (records.size() == 0)) {
               continue;
           }
           PeriodDataCallable callable = new PeriodDataCallable(yarnConfiguration, schema, fileName, records);
           pendingWrites.add(executor.submit(callable));
       }

       return new HashMap<Integer, List<GenericRecord>>();
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

    public static Integer getEarliestPeriod(Configuration yarnConfiguration, Table transactionTable) {
        try {
            String avroDir = transactionTable.getExtracts().get(0).getPath();
            avroDir = getPath(avroDir);
            log.info("Looking for earliest period table " + transactionTable.getName() + " path " + avroDir);
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
            Collections.sort(avroFiles);
            log.info("Get period from file " + avroFiles.get(0));
            return getPeriodFromFileName(avroFiles.get(0));
        } catch (Exception e) {
            log.error("Failed to find earlies period", e);
            return null;
        }
    }

    public static Map<String, Product> loadProductMap(Configuration yarnConfiguration, Table productTable) {
        Map<String, Product> productMap = new HashMap<>();
        for (Extract extract : productTable.getExtracts()) {
            List<GenericRecord> recordList = AvroUtils.getDataFromGlob(yarnConfiguration, extract.getPath());
            for (GenericRecord record : recordList) {
                Product product = new Product();
                product.setProductId(record.get(InterfaceName.ProductId.name()).toString());
                try {
                    product.setBundleId(record.get(InterfaceName.BundleId.name()).toString());
                } catch (Exception e) {
                    product.setBundleId(null);
                }
                product.setProductName(record.get(InterfaceName.ProductName.name()).toString());
                productMap.put(product.getProductId(), product);
            }
        }
        return productMap;
    }

    static class PeriodDataCallable implements Callable<Boolean> {
        private Configuration yarnConfiguration;
        private Schema schema;
        private String fileName;
        private List<GenericRecord> records;

        PeriodDataCallable(Configuration yarnConfiguration, Schema schema, String fileName, List<GenericRecord> records) {

            this.yarnConfiguration = yarnConfiguration;
            this.schema = schema;
            this.fileName = fileName;
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                log.info("Write " + records.size() + " records to " + fileName);
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

    private static PeriodBuilder getPeriodBuilder(PeriodStrategy strategy, String minDateStr) {
        // TODO: getting period builder by strategy and factory
        PeriodBuilder periodBuilder = new CalendarMonthPeriodBuilder();
        return periodBuilder;
    }

    private static Integer getPeriodFromFileName(String fileName) {
        try {
            return Integer.valueOf(fileName.split("-")[1]);
        } catch (Exception e) {
            return null;
        }
    }

    private static String getFileNameFromPeriod(Integer period) {
        return "Period-" + period + "-data.avro";
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

}
