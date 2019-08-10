package com.latticeengines.spark.testframework;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

public class TestPartitionTestNGBase extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TestPartitionTestNGBase.class);

    protected List<String> inputs;

    protected List<HdfsDataUnit> inputSources;

    @Override
    protected List<String> getInputOrder() {
        return inputs;
    }

    protected Function<HdfsDataUnit, Boolean> verifier;

    protected Integer dataCnt;

    @Override
    protected void verifyOutput(String output) {
        Assert.assertEquals(output, "This is my output!");
    }

    protected Integer uploadInputAvro() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Long.class), //
                Pair.of("Field1", String.class), //
                Pair.of("Field2", Long.class) ,//
                Pair.of("Field3", Float.class) ,//
                Pair.of("Field4", Double.class) , //
                Pair.of("Field5", Integer.class) //
        );
        Object[][] data = new Object[][] { //
                { 0L, "a", 1L ,	0.0f , 0.2d , 0 }, //
                { 1L, "b", 2L ,	0.1f , 0.2d , 0 }, //
                { 2L, "b", 3L ,	0.2f , 0.4d , 1 }, //
                { 3L, "c", 3L ,	0.1f , 0.3d , 4 }, //
                { 4L, "a", 2L ,	0.1f , 0.3d , 2 }, //
        };

        Object[][] preData = getData(data);

        String data0 = uploadHdfsDataUnit(preData, fields);

        inputs = Arrays.asList(data0);

        return preData.length;
    }

    private Object[][] getData(Object[][] data){
        Object[][] result = new Object[1000][];
        for(Integer i=0;i<1000;i++){
            Object[] row = data[i%5];
            row[0] = Long.valueOf(i);
            result[i]=row.clone();
        }
        return result;
    }

    protected void uploadOutputAsInput(List<HdfsDataUnit> inputs){
        try {
            if(CollectionUtils.isNotEmpty(inputs)) {
                this.inputs = hdfsOutputsAsInputs(inputs);
            }
            else{
                throw new RuntimeException("You have no inputs");
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Upload Output as Input fail.",e);
        }
    }

    protected Boolean verifyOutput1(HdfsDataUnit target) {
        List<String> keys = target.getPartitionKeys();
        Assert.assertEquals(keys, Arrays.asList("Field1","Field2","Field3","Field4","Field5"));
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            log.info("Record: {}",record);
            Assert.assertTrue(record.get("Id") instanceof Long);
        });
        Assert.assertEquals(count.get(), dataCnt.intValue());
        return true;
    }

    protected Boolean verifyOutput2(HdfsDataUnit target) {
        List<String> keys = target.getPartitionKeys();
        Assert.assertTrue(CollectionUtils.isEmpty(keys));
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(target).forEachRemaining(record -> {
            count.incrementAndGet();
            log.info("Record: {}",record);
            Assert.assertTrue(record.get("Id") instanceof Long);
            Assert.assertTrue(record.get("Field1") instanceof org.apache.avro.util.Utf8);
            Assert.assertTrue(record.get("Field2") instanceof Integer);
            Assert.assertTrue(record.get("Field3") instanceof Double);
            Assert.assertTrue(record.get("Field4") instanceof Double);
            Assert.assertTrue(record.get("Field5") instanceof Integer);
        });
        Assert.assertEquals(count.get(), dataCnt.intValue());
        return true;
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.singletonList(verifier);
    }
}
