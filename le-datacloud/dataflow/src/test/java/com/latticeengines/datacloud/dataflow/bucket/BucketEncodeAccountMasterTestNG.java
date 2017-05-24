package com.latticeengines.datacloud.dataflow.bucket;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.BucketEncode;
import com.latticeengines.datacloud.dataflow.utils.BucketEncodeUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;

public class BucketEncodeAccountMasterTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private final String ENC_ATTR = "EAttr280";
    private final int LOWEST_BIT = 40;
    private final int NUM_BITS = 2;
    private final long TEST_LATTICE_ID = 18599;
    private final int EXPECTED_BKT = 2;

    @Override
    protected String getFlowBeanName() {
        return BucketEncode.BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "AccountMaster";
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        BucketEncodeParameters parameters = getParameters();
        executeDataFlow(parameters);
        verifyResult();
    }

    private BucketEncodeParameters getParameters() {
        BucketEncodeParameters parameters = new BucketEncodeParameters();

        // read encoded attrs
        InputStream profileIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("calculateStats/AccountMaster/AccountMasterProfile/amprofile.avro");
        if (profileIs == null) {
            throw new RuntimeException("Failed ot find resource config.json");
        }
        List<GenericRecord> profileRecords;
        try {
            profileRecords = AvroUtils.readFromInputStream(profileIs);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read profile avro.", e);
        }
        parameters.encAttrs = BucketEncodeUtils.encodedAttrs(profileRecords);
        parameters.retainAttrs = BucketEncodeUtils.retainFields(profileRecords);
        parameters.renameFields = BucketEncodeUtils.renameFields(profileRecords);
        parameters.setBaseTables(Collections.singletonList("AccountMaster"));

        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        int numRows = 0;
        boolean checkNonEncodedAttrs = true;
        for (GenericRecord record : records) {
             long latticeId = (long) record.get("LatticeAccountId");
             if (latticeId == TEST_LATTICE_ID) {
                 System.out.println("Examining lattice id = " + TEST_LATTICE_ID);
                 long eattr = (long) record.get(ENC_ATTR);
                 int bkt = BitCodecUtils.getBits(eattr, LOWEST_BIT, NUM_BITS);
                 Assert.assertEquals(bkt, EXPECTED_BKT, String.format("id = %d, bkt = %d", (long) record.get("LatticeAccountId"), bkt));
             }
            numRows++;
            if (checkNonEncodedAttrs) {
                List<String> nonEncodedAttrs = new ArrayList<>();
                record.getSchema().getFields().forEach(field -> {
                    String attrName = field.name();
                    if (!attrName.startsWith("EAttr")) {
                        nonEncodedAttrs.add(attrName);
                    }
                });
                Assert.assertFalse(nonEncodedAttrs.contains("OUT_OF_BUSINESS_INDICATOR"));
                Assert.assertFalse(nonEncodedAttrs.contains("LE_IS_PRIMARY_DOMAIN"));
                checkNonEncodedAttrs = false;
            }
        }
        Assert.assertEquals(numRows, 1000);
    }

}
