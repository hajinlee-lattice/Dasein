package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TrimConfig;

public class TrimFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return TrimFlow.DATAFLOW_BEAN_NAME;
    }


    @Test(groups = "functional")
    public void testTrimFlow() {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("ContactId", String.class),
                Pair.of("CompanyName", String.class),
                Pair.of("FirstName", String.class), //
                Pair.of("Domain", String.class), //
                Pair.of("DUNS", String.class), //
                Pair.of("TransactionDayPeriod", Integer.class)//
        );
        Object[][] data = new Object[][] { //
                { "1", "1", " Test Company Name ", "Test First Name", "netapp.com", "DUNS11", 48485 }, //
                { "1", "2", " Test   Company Name ", "  Test First Name ", "netapp.com", "DUNS11", 48185 },
                { "2", "3", " Test  Company  Name ", " Test  First  Name ", "netapp.com", null, 48485 }, //
                { "3", "5", " Test Company Name", "Test   First Name", "netapp.com", "DUNS11", 48485 }, //
                { "4", "7", "Test Company  Name ", " Test First  Name   ", null, "DUNS14", 48485 }, //
                { "5", "9", "  Test Company  Name", "   Test First Name", null, "DUNS14", 48485 }, //
                { "6", "11", "  Test  Company  Name  ", " Test  First  Name", null, null, 48485 }, //
        };
        uploadDataToSharedAvroInput(data, fields);


        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT));
        TrimConfig config = new TrimConfig();
        config.setTrimColumns(Arrays.asList("City", "CompanyName"));
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 7);
        Set<String> firstNameSet = new HashSet<>();
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get("CompanyName").toString(), "Test Company Name");
            firstNameSet.add(record.get("FirstName").toString());
        }
        Assert.assertEquals(firstNameSet.size(), 7);
    }
}
