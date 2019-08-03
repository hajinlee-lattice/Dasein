package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.testframework.DataCloudEtlFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.TimeSeriesDistributer;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

public class TimeSeriesDistributerTestNG extends DataCloudEtlFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TimeSeriesDistributerTestNG.class);

    private static final int TOTAL = 100_000;
    private static final Integer MIN_PERIOD = 1;
    private static final Integer MAX_PERIOD = 20;
    private static final Set<Integer> PERIODS = IntStream.rangeClosed(MIN_PERIOD, MAX_PERIOD).boxed()
            .collect(Collectors.toSet());
    private static final String[] PERIOD_NAMES = { PeriodStrategy.Template.Week.name(),
            PeriodStrategy.Template.Month.name() };
    @SuppressWarnings("serial")
    private static final List<Pair<String, Class<?>>> SCHEMA = new ArrayList<Pair<String, Class<?>>>() {
        {
            add(Pair.of(InterfaceName.Id.name(), String.class));
            add(Pair.of(InterfaceName.PeriodName.name(), String.class));
            add(Pair.of(InterfaceName.PeriodId.name(), Integer.class));
        }
    };

    @Test(groups = "functional")
    public void testSinglePeriodStrategy() {
        String baseDir = "/tmp/" + this.getClass().getSimpleName() + "/SinglePeriodStrategy";
        cleanupHdfsPath(baseDir);
        String inputDir = baseDir + "/Input";
        Object[][] data = prepareSinglePeriodStrategyData();
        uploadInputData(data, inputDir);
        String targetDir = baseDir + "/Output";

        @SuppressWarnings("serial")
        TimeSeriesDistributer distributer = new TimeSeriesDistributer.DistributerBuilder() //
                .yarnConfig(yarnConfiguration) //
                .inputDir(inputDir) //
                .targetDirs(new HashMap<String, String>() {
                    {
                        put(TimeSeriesDistributer.DUMMY_PERIOD, targetDir);
                    }
                }) //
                .periods(new HashMap<String, Set<Integer>>() {
                    {
                        put(TimeSeriesDistributer.DUMMY_PERIOD, PERIODS);
                    }
                }) //
                .periodField(InterfaceName.PeriodId.name()) //
                .periodNameField(null) //
                .build();
        distributer.distributePeriodData();

        verifyOutputData(data, targetDir, null);
        cleanupHdfsPath(baseDir);
    }

    private Object[][] prepareSinglePeriodStrategyData() {
        Random random = new Random();
        Object[][] arr = new Object[TOTAL][SCHEMA.size()];
        for (int i = 0; i < TOTAL; i++) {
            arr[i][0] = String.valueOf(i);
            arr[i][1] = null;
            arr[i][2] = random.nextInt(MAX_PERIOD + 1 - MIN_PERIOD) + MIN_PERIOD;
        }
        return arr;
    }

    @Test(groups = "functional")
    public void testMultiPeriodStrategy() {
        String baseDir = "/tmp/" + this.getClass().getSimpleName() + "/MultiPeriodStrategy";
        cleanupHdfsPath(baseDir);
        String inputDir = baseDir + "/Input";
        Object[][] data = prepareMultiPeriodStrategyData();
        uploadInputData(data, inputDir);
        String targetDir = baseDir + "/Output";
        Map<String, String> targetDirs = new HashMap<>();
        for (String periodName : PERIOD_NAMES) {
            targetDirs.put(periodName, targetDir + "/" + periodName);
        }
        Map<String, Set<Integer>> periods = new HashMap<>();
        for (String periodName : PERIOD_NAMES) {
            periods.put(periodName, PERIODS);
        }

        TimeSeriesDistributer distributer = new TimeSeriesDistributer.DistributerBuilder() //
                .yarnConfig(yarnConfiguration) //
                .inputDir(inputDir) //
                .targetDirs(targetDirs) //
                .periods(periods) //
                .periodField(InterfaceName.PeriodId.name()) //
                .periodNameField(InterfaceName.PeriodName.name()) //
                .build();
        distributer.distributePeriodData();

        for (String periodName : PERIOD_NAMES) {
            Object[][] periodData = filterInputByPeriod(data, periodName);
            verifyOutputData(periodData, targetDirs.get(periodName), periodName);
        }
        cleanupHdfsPath(baseDir);
    }

    // Result verification is based on assumption that total size is large
    // enough that every PeriodName covers all the PeriodIds
    private Object[][] prepareMultiPeriodStrategyData() {
        Random random = new Random();
        Object[][] arr = new Object[TOTAL][SCHEMA.size()];
        for (int i = 0; i < TOTAL; i++) {
            arr[i][0] = String.valueOf(i);
            arr[i][1] = PERIOD_NAMES[random.nextInt(2)];
            arr[i][2] = random.nextInt(MAX_PERIOD + 1 - MIN_PERIOD) + MIN_PERIOD;
        }
        return arr;
    }

    private Object[][] filterInputByPeriod(Object[][] input, String periodName) {
        int periodNameFldIdx = IntStream.range(0, SCHEMA.size()) //
                .filter(i -> InterfaceName.PeriodName.name().equals(SCHEMA.get(i).getLeft())) //
                .findFirst() //
                .orElse(-1);
        if (periodNameFldIdx == -1) {
            throw new RuntimeException("Cannot find PeriodName field in fields");
        }
        return Arrays.stream(input) //
                .filter(row -> periodName.equals(row[periodNameFldIdx]))
                .toArray(size -> new Object[size][input[0].length]);
    }

    private void cleanupHdfsPath(String path) {
        try {
            HdfsUtils.rmdir(yarnConfiguration, path);
        } catch (IOException e) {
            throw new RuntimeException("Fail to cleanup HDFS path " + path);
        }
    }

    private void uploadInputData(Object[][] data, String inputDir) {
        try {
            int seq = 0;
            int batchSize = TOTAL / 10;
            while (seq < data.length) {
                int length = Math.min(batchSize, data.length - seq);
                Object[][] toUpload = new Object[length][data[0].length];
                System.arraycopy(data, seq, toUpload, 0, length);
                AvroUtils.createAvroFileByData(yarnConfiguration, SCHEMA, toUpload, inputDir,
                        "part-" + seq + ".avro");
                seq += length;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyOutputData(Object[][] data, String targetDir, String periodName) {
        Map<String, List<Object>> expectedMap = Arrays.stream(data)
                .collect(Collectors.toMap(arr -> (String) arr[0], arr -> Arrays.asList(arr)));
        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, targetDir);
        } catch (IOException e) {
            throw new RuntimeException("Fail to list files under target dir " + targetDir);
        }
        Set<Integer> actualPeriodIds = new HashSet<>();

        for (String file : files) {
            AvroFilesIterator avroIter = AvroUtils.iterateAvroFiles(yarnConfiguration, file);
            Integer periodId = TimeSeriesUtils.getPeriodFromFileName(file);
            actualPeriodIds.add(periodId);
            while (avroIter.hasNext()) {
                GenericRecord record = avroIter.next();
                Assert.assertNotNull(record);
                // Verify PeriodId in record same as PeriodId in file name
                Assert.assertEquals(record.get(InterfaceName.PeriodId.name()), periodId);
                // Verify PeriodName in record same as PeriodStore
                if (periodName != null) {
                    Assert.assertNotNull(record.get(InterfaceName.PeriodName.name()));
                    Assert.assertEquals(record.get(InterfaceName.PeriodName.name()).toString(), periodName);
                }
                // Verify record data correctness
                Assert.assertNotNull(record.get(InterfaceName.Id.name()));
                String id = record.get(InterfaceName.Id.name()).toString();
                List<Object> expected = expectedMap.get(id);
                Assert.assertNotNull(expected);
                List<Object> actual = SCHEMA.stream().map(pair -> pair.getLeft()) //
                        .map(field -> record.get(field) == null ? null
                                : (record.get(field) instanceof Utf8 ? record.get(field).toString()
                                        : record.get(field)))
                        .collect(Collectors.toList());
                Assert.assertEquals(actual, expected);
                expectedMap.remove(id);
            }
        }
        Assert.assertTrue(expectedMap.isEmpty());
        Assert.assertEquals(actualPeriodIds, PERIODS);
    }

}
