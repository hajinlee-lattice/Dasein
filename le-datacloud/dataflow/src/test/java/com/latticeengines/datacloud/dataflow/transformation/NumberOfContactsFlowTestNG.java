package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class NumberOfContactsFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(NumberOfContactsFlowTestNG.class);

    protected static final String ACCOUNT = SchemaInterpretation.Account.name();
    protected static final String CONTACT = SchemaInterpretation.Contact.name();

    @Override
    protected String getFlowBeanName() {
        return NumberOfContactsFlow.DATAFLOW_BEAN_NAME;
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

        // Set up the input tables.  First the Account Table.
        List<Pair<String, Class<?>>> accountFields = Arrays.asList(
                Pair.of("AccountId", String.class),
                Pair.of("Company", String.class),
                Pair.of("Country", String.class)
        );
        Object[][] accountData = new Object[][]{
                { "1", "Google", "USA"},
                { "2", "Facebook", "USA"},
                { "3", "Salesforce", "USA" },
                { "4", "Samsung", "South Korea" },
                { "5", "Toshiba", "Japan" },
                { "6", "BlackBerry", "Canada" }
        };
        uploadAvro(accountData, accountFields, ACCOUNT, "/tmp/account");

        // Now the Contact Table.
        List<Pair<String, Class<?>>> contactFields = Arrays.asList(
                Pair.of("ContactId", String.class),
                Pair.of("AccountId", String.class),
                Pair.of("FirstName", String.class),
                Pair.of("LastName", String.class)
        );
        Object[][] contactData = new Object[][] {
                { "1", "1", "Eric", "Schmidt" },
                { "2", "1", "Larry", "Page" },
                { "3", "1", "Sergey", "Brin" },
                { "4", "2", "Mark", "Zuckerberg" },
                { "5", "2", "Sheryl", "Sandberg" },
                { "6", "4", "Jon", "Snow" },
                { "7", "5", "Daenerys", "Targaryen" },
                { "7", "5", "Tyrion", "Lanister" }
        };
        uploadAvro(contactData, contactFields, CONTACT, "/tmp/contact");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setConfJson(JsonUtils.serialize(config));
        parameters.setBaseTables(Arrays.asList(ACCOUNT, CONTACT));
        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        Map<String, String> sourcePathMap = new HashMap<>();
        sourcePathMap.put(ACCOUNT, "/tmp/account/" + ACCOUNT + ".avro");
        sourcePathMap.put(CONTACT, "/tmp/contact/" + CONTACT + ".avro");
        return sourcePathMap;
    }

    private Object[][] expectedOutputTable = new Object[][] {
            // AccountId, NumberOfContacts
            {  "1",       3 },
            {  "2",       2 },
            {  "3",       0 },
            {  "4",       1 },
            {  "5",       2 },
            {  "6",       0 }
    };

    private void verifyResult() {
        List<GenericRecord> records = readOutput();

        // For debugging, output the output table first.
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            log.info("Output Record " + i + ": " + record);
        }

        Assert.assertEquals(records.size(), 6);
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            Object[] expectedRecord = expectedOutputTable[i];

            String accountId = ((Utf8) record.get(NumberOfContactsFlow.ACCOUNT_ID)).toString();
            Integer numberOfContacts = (Integer) record.get(NumberOfContactsFlow.NUMBER_OF_CONTACTS);

            Assert.assertEquals(accountId, expectedRecord[0], "Mismatch for record " + i + ": AccoundId");
            Assert.assertEquals(numberOfContacts, expectedRecord[1],
                    "Mismatch for record " + i + ": NumberOfContacts");
        }
    }
}


