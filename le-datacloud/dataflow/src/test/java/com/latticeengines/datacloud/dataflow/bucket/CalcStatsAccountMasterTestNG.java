package com.latticeengines.datacloud.dataflow.bucket;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class CalcStatsAccountMasterTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return CalculateStats.BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "AccountMaster";
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = getParameters();
        executeDataFlow(parameters);
        verifyResult();
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            String attrName = record.get("AttrName").toString();
            if (attrName.startsWith("TechIndicator")) {
                String[] bkts = record.get("BktCounts").toString().split("\\|");
                for (String bkt : bkts) {
                    String[] tokens = bkt.split(":");
                    int bktId = Integer.valueOf(tokens[0]);
                    Assert.assertTrue(bktId >= 0 && bktId < 3, "Found an invalid bkt id " + bktId);
                }
            }
        }
    }

    private TransformationFlowParameters getParameters() {
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList("AccountMasterBucketed", "AccountMasterProfile"));
        return parameters;
    }

}
