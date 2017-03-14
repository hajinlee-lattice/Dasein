package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy.BOOLEAN_YESNO;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;

public class BucketEncodeTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return BucketEncode.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        BucketEncodeParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private BucketEncodeParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("RowID", Long.class), //
                Pair.of("RelayString", String.class), //
                Pair.of("RelayInteger", Integer.class), //
                Pair.of("IntervalInt", Integer.class), //
                Pair.of("IntervalDouble", Double.class), //
                Pair.of("CatString", String.class), //
                Pair.of("CatMapString", String.class), //
                Pair.of("Boolean1", String.class), //
                Pair.of("Boolean2", String.class), //
                Pair.of("Boolean3", Integer.class), //
                Pair.of("Boolean4", Boolean.class), //
                Pair.of("Encoded", String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1L, "String1", 1, 1, 11.0, "Value2", "Group3A", "Y", "0", 1, true, createEncodedString() }
        };
        uploadDataToSharedAvroInput(data, fields);

        BucketEncodeParameters parameters = new BucketEncodeParameters();

        // construct encoded attrs
        DCEncodedAttr attr1 = new DCEncodedAttr("EAttr1");

        DCBucketedAttr bktAttr11 = new DCBucketedAttr("IntervalInt", 0, 3);
        IntervalBucket intervalBucket = new IntervalBucket();
        intervalBucket.setBoundaries(Arrays.asList(0, 10, 100));
        bktAttr11.setBucketAlgo(intervalBucket);
        attr1.addBktAttr(bktAttr11);

        DCBucketedAttr bktAttr12 = new DCBucketedAttr("IntervalDouble", 10, 3);
        bktAttr12.setBucketAlgo(intervalBucket);
        attr1.addBktAttr(bktAttr12);

        DCEncodedAttr attr2 = new DCEncodedAttr("EAttr2");

        DCBucketedAttr bktAttr21 = new DCBucketedAttr("CatString", 3, 3);
        CategoricalBucket categoricalBucket1 = new CategoricalBucket();
        categoricalBucket1.setCategories(Arrays.asList("Value1", "Value2", "Value2"));
        bktAttr21.setBucketAlgo(categoricalBucket1);
        attr2.addBktAttr(bktAttr21);

        DCBucketedAttr bktAttr22 = new DCBucketedAttr("CatMapString", 6, 3);
        CategoricalBucket categoricalBucket2 = new CategoricalBucket();
        categoricalBucket2.setCategories(Arrays.asList("Group1", "Group2", "Group3"));
        Map<String, List<String>> mapping = new HashMap<>();
        mapping.put("Group1", Arrays.asList("Group1A", "Group1B"));
        mapping.put("Group2", Arrays.asList("Group2A", "Group2B"));
        mapping.put("Group3", Arrays.asList("Group3A", "Group3B"));
        categoricalBucket2.setMapping(mapping);
        bktAttr22.setBucketAlgo(categoricalBucket2);
        attr2.addBktAttr(bktAttr22);

        DCBucketedAttr bktAttr23 = new DCBucketedAttr("Boolean1", 10, 3);
        BooleanBucket booleanBucket = new BooleanBucket();
        bktAttr23.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr23);

        DCBucketedAttr bktAttr24 = new DCBucketedAttr("Boolean2", 13, 3);
        bktAttr24.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr24);

        DCBucketedAttr bktAttr25 = new DCBucketedAttr("Boolean3", 16, 3);
        bktAttr25.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr25);

        DCBucketedAttr bktAttr26 = new DCBucketedAttr("Boolean4", 19, 3);
        bktAttr26.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr26);

        DCEncodedAttr attr3 = new DCEncodedAttr("EAttr3");

        DCBucketedAttr bktAttr31 = new DCBucketedAttr("BitEncodeYes", 4, 2);
        bktAttr31.setBucketAlgo(booleanBucket);
        BitDecodeStrategy decodeStrategy1 = new BitDecodeStrategy();
        decodeStrategy1.setBitInterpretation(BOOLEAN_YESNO);
        decodeStrategy1.setBitPosition(3);
        decodeStrategy1.setEncodedColumn("Encoded");
        bktAttr31.setDecodedStrategy(decodeStrategy1);
        attr3.addBktAttr(bktAttr31);

        DCBucketedAttr bktAttr32 = new DCBucketedAttr("BitEncodeNo", 14, 2);
        bktAttr32.setBucketAlgo(booleanBucket);
        BitDecodeStrategy decodeStrategy2 = new BitDecodeStrategy();
        decodeStrategy2.setBitInterpretation(BOOLEAN_YESNO);
        decodeStrategy2.setBitPosition(1026);
        decodeStrategy2.setEncodedColumn("Encoded");
        bktAttr32.setDecodedStrategy(decodeStrategy2);
        attr3.addBktAttr(bktAttr32);

        parameters.encAttrs = Arrays.asList(attr1, attr2, attr3);
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.rowIdField = "RowID";

        return parameters;
    }

    private String createEncodedString() {
        try {
            return BitCodecUtils.encode(new int[]{ 3, 5});
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate encoded string.", e);
        }
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            System.out.println(String.format("IntervalInt=%d, " + //
                            "IntervalDlb=%d, " + //
                            "CatString=%d, " + //
                            "CatMapString=%d, " + //
                            "Boolean1=%d, " + //
                            "Boolean2=%d, " + //
                            "Boolean3=%d, " + //
                            "Boolean4=%d, " + //
                            "BitEncode1=%d, " + //
                            "BitEncode2=%d", //
                    getIntervalIntBkt(record), //
                    getIntervalDlbBkt(record), //
                    getCatStringBkt(record), //
                    getCatMapStringBkt(record), //
                    getBoolean1Bkt(record), //
                    getBoolean2Bkt(record), //
                    getBoolean3Bkt(record), //
                    getBoolean4Bkt(record), //
                    getBitEncodeYesBkt(record), //
                    getBitEncodeNoBkt(record) //
                    ));
            Assert.assertEquals(getIntervalIntBkt(record), 2);
            Assert.assertEquals(getIntervalDlbBkt(record), 3);
            Assert.assertEquals(getCatStringBkt(record), 2);
            Assert.assertEquals(getCatMapStringBkt(record), 3);
            Assert.assertEquals(getBoolean1Bkt(record), 1);
            Assert.assertEquals(getBoolean2Bkt(record), 2);
            Assert.assertEquals(getBoolean3Bkt(record), 1);
            Assert.assertEquals(getBoolean4Bkt(record), 1);
            Assert.assertEquals(getBitEncodeYesBkt(record), 1);
            Assert.assertEquals(getBitEncodeNoBkt(record), 2);
        }
    }

    private int getIntervalIntBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr1");
        return BitCodecUtils.getBits(encoded, 0, 3);
    }

    private int getIntervalDlbBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr1");
        return BitCodecUtils.getBits(encoded, 10, 3);
    }

    private int getCatStringBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 3, 3);
    }

    private int getCatMapStringBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 6, 3);
    }

    private int getBoolean1Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 10, 3);
    }

    private int getBoolean2Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 13, 3);
    }

    private int getBoolean3Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 16, 3);
    }

    private int getBoolean4Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 19, 3);
    }

    private int getBitEncodeYesBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr3");
        return BitCodecUtils.getBits(encoded, 4, 2);
    }

    private int getBitEncodeNoBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr3");
        return BitCodecUtils.getBits(encoded, 14, 2);
    }

}
