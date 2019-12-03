package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class NumberOfContactsFlowDP7532TestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(NumberOfContactsFlowDP7532TestNG.class);

    protected static final String ACCOUNT = SchemaInterpretation.Account.name();
    protected static final String CONTACT = SchemaInterpretation.Contact.name();

    @Override
    protected String getFlowBeanName() {
        return NumberOfContactsFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "DP7532";
    }


    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = generateParameters();
        executeDataFlow(parameters);
        verifyResult();
    }

    TransformationFlowParameters generateParameters() {

        // Set up configuration.  For now, the only configuration is to set the key (field) for joining the two input
        // tables.
        NumberOfContactsConfig config = new NumberOfContactsConfig();
        config.setLhsJoinField("AccountId");
        config.setRhsJoinField("AccountId");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setConfJson(JsonUtils.serialize(config));
        parameters.setBaseTables(Arrays.asList(ACCOUNT, CONTACT));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();

        // For debugging, output the output table first.
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            log.info("Output Record " + i + ": " + record);
        }

        Assert.assertEquals(records.size(), 109);
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);

            String accountId = ((Utf8) record.get(NumberOfContactsFlow.ACCOUNT_ID)).toString();
            Integer numberOfContacts = (Integer) record.get(NumberOfContactsFlow.NUMBER_OF_CONTACTS);

            if (accountId.equals("0012400001DNwcNAAT")) {
                Assert.assertEquals(numberOfContacts.intValue(), 1,
                        "Mismatch for record " + i + " with AccountID " + accountId + ": NumberOfContacts");
            } else {
                Assert.assertEquals(numberOfContacts.intValue(), 0,
                        "Mismatch for record " + i + " with AccountID " + accountId + ": NumberOfContacts");
            }
        }
    }
}


