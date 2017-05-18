package com.latticeengines.datacloud.dataflow.bucket;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.CalculateStats;
import com.latticeengines.domain.exposed.datacloud.dataflow.CalculateStatsParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;

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
        CalculateStatsParameter parameters = getParameters();
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
                for(String bkt: bkts) {
                    String[] tokens = bkt.split(":");
                    int bktId = Integer.valueOf(tokens[0]);
                    Assert.assertTrue(bktId >= 0 && bktId < 3, "Found an invalid bkt id " + bktId);
                }
            }
        }
    }

    private CalculateStatsParameter getParameters() {
        // read encoded attrs
        InputStream encAttrsIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(getDirectory() + File.separator + "config.json");
        if (encAttrsIs == null) {
            throw new RuntimeException("Failed ot find resource config.json");
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<List<DCEncodedAttr>> typeRef = new TypeReference<List<DCEncodedAttr>>() { };
        List<DCEncodedAttr> encAttrs;
        try {
            encAttrs = objectMapper.readValue(encAttrsIs, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse json config.", e);
        }

        CalculateStatsParameter parameters = new CalculateStatsParameter();
        parameters.encAttrs = encAttrs;
        parameters.setBaseTables(Collections.singletonList("AccountMasterBucketed"));
        parameters.ignoreAttrs = Collections.singletonList("LatticeAccountId");

        return parameters;
    }

}
