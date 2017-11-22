package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.period.CalendarMonthPeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodBuilder;
import com.latticeengines.common.exposed.period.PeriodStrategy;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class TimeSeriesUtils {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesUtils.class);

    public static Set<Integer> collectPeriods(YarnConfiguration yarnConfiguration, String avroDir, String periodField) {

        avroDir = getPath(avroDir) + "/*.avro";
        log.info("Collect " + periodField + " periods from " + avroDir);
        Set<Integer> periodSet = new HashSet<Integer>();
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, avroDir);
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer)record.get(periodField);
                periodSet.add(period);
            }
        } catch (Exception e) {
            log.error("Failed to collect periods for period table", e);
        }

        return periodSet;
    }

    public static void cleanupPeriodData(YarnConfiguration yarnConfiguration, String avroDir, Set<Integer> periods) {
        avroDir = getPath(avroDir);
        log.info("Clean periods from " + avroDir);
        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
            for (String fileName : avroFiles) {
                try {
                    if (periods == null) {
                       HdfsUtils.rmdir(yarnConfiguration, fileName);
                    } else {
                        String[] dirs = fileName.split("/");
                        String avroName = dirs[dirs.length - 1];
                        Integer period = getPeriodFromFileName(avroName);
                        if ((period != null) && periods.contains(period)) {
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

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir, String avroDir, Set<Integer> periods) {
        collectPeriodData(yarnConfiguration, targetDir, avroDir, periods, null, null);

    }

    public static void collectPeriodData(YarnConfiguration yarnConfiguration, String targetDir, String avroDir, Set<Integer> periods,
                                         PeriodStrategy periodStrategy, String earliestTransaction) {
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
                    period = new Integer(periodBuilder.toPeriodId(DateTimeUtils.dayPeriodToDate(period)));
                }
                if ((period != null) && periods.contains(period)) {
                   HdfsUtils.copyFiles(yarnConfiguration, fileName, targetDir);
                }
            }
        } catch (Exception e) {
            log.error("Failed to collect period data", e);
        }
    }

    public static boolean distributePeriodData(YarnConfiguration yarnConfiguration, String inputDir, String targetDir, Set<Integer> periods, String periodField) {
        inputDir = getPath(inputDir) + "/*.avro";
        targetDir = getPath(targetDir);
        log.info("Dritribute period data from " + inputDir + " to " + targetDir);
        Map<Integer, String> periodFileMap = new HashMap<Integer, String>();
        for (Integer period : periods) {
            periodFileMap.put(period, targetDir + "/" + getFileNameFromPeriod(period));
            log.info("Add period " + period + " File" + targetDir + "/" + getFileNameFromPeriod(period));
        }
        try {
            Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, inputDir);
            Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, inputDir);
            Map<Integer, List<GenericRecord>> dateRecordMap = new HashMap<>();
            while (iter.hasNext()) {
                GenericRecord record = iter.next();
                Integer period = (Integer)record.get(periodField);
                if (!dateRecordMap.containsKey(period)) {
                    dateRecordMap.put(period, new ArrayList<>());
                }
                dateRecordMap.get(period).add(record);
                if (dateRecordMap.get(period).size() >= 128) {
                    writeDataBuffer(yarnConfiguration, schema, period, periodFileMap, dateRecordMap);
                }
            }
            for (Map.Entry<Integer, List<GenericRecord>> entry : dateRecordMap.entrySet()) {
                writeDataBuffer(yarnConfiguration, schema, entry.getKey(), periodFileMap, dateRecordMap);
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to distribute to period store", e);
            return false;
        }
    }

    public static Integer getEarliestPeriod(YarnConfiguration yarnConfiguration, Table transactionTable) {
        try {
            String avroDir = transactionTable.getExtracts().get(0).getPath();
            avroDir = getPath(avroDir);
            log.info("Looking for earliest period table " + transactionTable.getName() + " path " + avroDir); 
            List<String>  avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*.avro$");
            Collections.sort(avroFiles);
            log.info("Get period from file " + avroFiles.get(0));
            return getPeriodFromFileName(avroFiles.get(0));
         } catch (Exception e) {
            log.error("Failed to find earlies period", e);
            return null;
         }
    }

    public static Map<String, Product> loadProductMap(YarnConfiguration yarnConfiguration, Table productTable) {
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

    private static void writeDataBuffer(YarnConfiguration yarnConfiguration, Schema schema, Integer period, Map<Integer, String> periodFileMap,
            Map<Integer, List<GenericRecord>> dateRecordMap) throws IOException {
        List<GenericRecord> records = dateRecordMap.get(period);
        String fileName = periodFileMap.get(period);
        if (fileName == null) {
            log.info("Failed to find file for " + period);
        }
        if (!HdfsUtils.fileExists(yarnConfiguration, fileName)) {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, fileName, records);
        } else {
            AvroUtils.appendToHdfsFile(yarnConfiguration, fileName, records);
        }
        records.clear();

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
}
