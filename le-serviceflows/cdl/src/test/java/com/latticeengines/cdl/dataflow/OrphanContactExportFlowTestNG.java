package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanContactExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class OrphanContactExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    private static final String ACCOUNT_TABLE = "AccountTable";
    private static final String CONTACT_TABLE = "ContactTable";
    private static final String ACCOUNT_DIR = "/tmp/OrphanContactExportFlowTestNG/account/";
    private static final String CONTACT_DIR = "/tmp/OrphanContactExportFlowTestNG/contact/";

    private Object[][] accountData = new Object[][] {
            // "AccountId", "Name"
            { "A001", "Husky" },
            { "A002", "Alaskan Malamute" },
            { "A003", "Collie" },
            { "A004", "Chihuahua" },
            { "A005", "Labrador Retriever" }
    };

    private Object[][] contactData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C001", "contact_1", "A001" },
            { "C002", "contact_2", "A002" },
            { "C003", "contact_3", "A003" },
            { "C004", "contact_4", "A003" },
            { "C005", "contact_5", "A004" },
            { "C006", "contact_6", "A004" },
            { "C007", "contact_7", "A001" },
            { "C008", "contact_8", "A999" },
            { "C009", "contact_9", "A001" },
            { "C010", "contact_10", "A888" }
    };

    private Object[][] expectedData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C010", "contact_10", "A888" },
            { "C008", "contact_8", "A999" }
    };

    private Object[][] expectNullData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C001", "contact_1", "A001" },
            { "C002", "contact_2", "A002" },
            { "C003", "contact_3", "A003" },
            { "C004", "contact_4", "A003" },
            { "C005", "contact_5", "A004" },
            { "C006", "contact_6", "A004" },
            { "C007", "contact_7", "A001" },
            { "C008", "contact_8", "A999" },
            { "C009", "contact_9", "A001" },
            { "C010", "contact_10", "A888" }
    };

    @Test(groups = "functional")
    public void testOrphanContacts() {
        OrphanContactExportParameters parameters = prepareInput(accountData, contactData);
        executeDataFlow(parameters);
        verifyResult(expectedData,2);
    }

    @Test(groups = "functional")
    public void testOrphanContactsWithNullAccountTable() {
        OrphanContactExportParameters parameters = prepareInput(null, contactData);
        executeDataFlow(parameters);
        verifyResult(expectNullData,10);
    }

    @Override
    protected String getFlowBeanName() {
        return OrphanContactExportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(
                ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro",
                CONTACT_TABLE, CONTACT_DIR + CONTACT_TABLE + ".avro");
    }

    private List<Pair<String, Class<?>>> prepareAccountData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Name.name(), String.class));
        return columns;
    }

    private List<Pair<String, Class<?>>> prepareContactData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.ContactId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ContactName.name(), String.class));
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        return columns;
    }

    private OrphanContactExportParameters prepareInput(Object[][] accountData, Object[][] contactData) {
        OrphanContactExportParameters parameters = new OrphanContactExportParameters();
        if (accountData != null) {
            uploadAvro(accountData, prepareAccountData(), ACCOUNT_TABLE, ACCOUNT_DIR);
            parameters.setAccountTable(ACCOUNT_TABLE);
        }
        uploadAvro(contactData, prepareContactData(), CONTACT_TABLE, CONTACT_DIR);
        parameters.setContactTable(CONTACT_TABLE);
        parameters.setValidatedColumns(Arrays.asList(InterfaceName.ContactId.name(), InterfaceName.ContactName.name(),
                InterfaceName.AccountId.name()));
        return parameters;
    }

    public void verifyResult(Object[][] expectedData, int expectNumOfRows) {
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get(InterfaceName.ContactId.name()).toString(), expectedData[rowNum][0]);
            Assert.assertEquals(record.get(InterfaceName.ContactName.name()).toString(), expectedData[rowNum][1]);
            Assert.assertEquals(record.get(InterfaceName.AccountId.name()).toString(), expectedData[rowNum][2]);
            rowNum ++;
        }
        Assert.assertEquals(rowNum, expectNumOfRows);
    }
}
