package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBitEncodeNoBkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBitEncodeYesBkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBoolean1Bkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBoolean2Bkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBoolean3Bkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getBoolean4Bkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getCatMapStringBkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getCatStringBkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getIntervalDlbBkt;
import static com.latticeengines.datacloud.dataflow.bucket.BucketTestUtils.getIntervalIntBkt;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.BucketEncode;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;

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
        parameters.encAttrs = BucketTestUtils.EncodedAttributes();
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
            if ((long) record.get("RowID") == 1) {
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
    }

}
