package com.latticeengines.cdl.dataflow;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;

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
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanContactExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class OrphanContactExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    private static final String ACCOUNT_TABLE = "AccountTable";
    private static final String ACCOUNT_EM_TABLE = "AccountEMTable";
    private static final String CONTACT_TABLE = "ContactTable";
    private static final String CONTACT_EM_TABLE = "ContactEMTable";
    private static final String ACCOUNT_DIR = "/tmp/OrphanContactExportFlowTestNG/account/legacy/";
    private static final String ACCOUNT_EM_DIR = "/tmp/OrphanContactExportFlowTestNG/account/em/";
    private static final String CONTACT_DIR = "/tmp/OrphanContactExportFlowTestNG/contact/legacy/";
    private static final String CONTACT_EM_DIR = "/tmp/OrphanContactExportFlowTestNG/contact/em/";

    private Object[][] accountData = new Object[][] {
            // "AccountId", "Name"
            { "A001", "Husky" }, //
            { "A002", "Alaskan Malamute" }, //
            { "A003", "Collie" }, //
            { "A004", "Chihuahua" }, //
            { "A005", "Labrador Retriever" } //
    };

    private Object[][] accountEMData = new Object[][] {
            // "AccountId", "Name", "CustomerAccountId"
            { "A001", "Husky", "CA001" }, //
            { "A002", "Alaskan Malamute", "CA002" }, //
            { "A003", "Collie", "CA003" }, //
            { "A004", "Chihuahua", "CA004" }, //
            { "A005", "Labrador Retriever", "CA005" } //
    };

    private Object[][] contactData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C001", "contact_1", "A001" }, //
            { "C002", "contact_2", "A002" }, //
            { "C003", "contact_3", "A003" }, //
            { "C004", "contact_4", "A003" }, //
            { "C005", "contact_5", "A004" }, //
            { "C006", "contact_6", "A004" }, //
            { "C007", "contact_7", "A001" }, //
            { "C008", "contact_8", "A999" }, //
            { "C009", "contact_9", "A001" }, //
            { "C010", "contact_10", "A888" } //
    };

    private Object[][] contactEMData = new Object[][] {
            // "ContactId", "ContactName", "AccountId", "CustomerContactId",
            // "CustomerAccountId"
            { "C001", "contact_1", "A001", "CC001", "CA001" }, //
            { "C002", "contact_2", "A002", "CC002", "CA002" }, //
            { "C003", "contact_3", "A003", "CC003", "CA003" }, //
            { "C004", "contact_4", "A003", "CC004", "CA004" }, //
            { "C005", "contact_5", "A004", "CC005", "CA005" }, //
            { "C006", "contact_6", "A004", "CC006", "CA006" }, //
            { "C007", "contact_7", "A001", "CC007", "CA007" }, //
            { "C008", "contact_8", "A999", "CC008", "CA008" }, //
            { "C009", "contact_9", "A001", "CC009", "CA009" }, //
            { "C010", "contact_10", "A888", "CC010", "CA010" } //
    };

    private Object[][] expectedData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C010", "contact_10", "A888" }, //
            { "C008", "contact_8", "A999" } //
    };

    private Object[][] expectNullData = new Object[][] {
            // "ContactId", "ContactName", "AccountId"
            { "C001", "contact_1", "A001" }, //
            { "C002", "contact_2", "A002" }, //
            { "C003", "contact_3", "A003" }, //
            { "C004", "contact_4", "A003" }, //
            { "C005", "contact_5", "A004" }, //
            { "C006", "contact_6", "A004" }, //
            { "C007", "contact_7", "A001" }, //
            { "C008", "contact_8", "A999" }, //
            { "C009", "contact_9", "A001" }, //
            { "C010", "contact_10", "A888" } //
    };

    @Test(groups = "functional")
    public void testOrphanContacts() {
        OrphanContactExportParameters parameters = prepareInput(accountData, contactData, false);
        executeDataFlow(parameters);
        verifyResult(expectedData,2);
    }

    @Test(groups = "functional")
    public void testOrphanContactsWithNullAccountTable() {
        OrphanContactExportParameters parameters = prepareInput(null, contactData, false);
        executeDataFlow(parameters);
        verifyResult(expectNullData,10);
    }

    @Test(groups = "functional")
    public void testOrphanContactsEM() {
        OrphanContactExportParameters parameters = prepareInput(accountEMData, contactEMData, true);
        executeDataFlow(parameters);
        verifyResult(expectedData, 2);
    }

    @Override
    protected String getFlowBeanName() {
        return OrphanContactExportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(
                ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro", //
                CONTACT_TABLE, CONTACT_DIR + CONTACT_TABLE + ".avro", //
                ACCOUNT_EM_TABLE, ACCOUNT_EM_DIR + ACCOUNT_EM_TABLE + ".avro", //
                CONTACT_EM_TABLE, CONTACT_EM_DIR + CONTACT_EM_TABLE + ".avro" //
        );
    }

    private List<Pair<String, Class<?>>> prepareAccountData(boolean entityMatchEnabled) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(AccountId.name(), String.class));
        columns.add(Pair.of(Name.name(), String.class));
        if (entityMatchEnabled) {
            columns.add(Pair.of(CustomerAccountId.name(), String.class));
        }
        return columns;
    }

    private List<Pair<String, Class<?>>> prepareContactData(boolean entityMatchEnabled) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(ContactId.name(), String.class));
        columns.add(Pair.of(ContactName.name(), String.class));
        columns.add(Pair.of(AccountId.name(), String.class));
        if (entityMatchEnabled) {
            columns.add(Pair.of(CustomerAccountId.name(), String.class));
            columns.add(Pair.of(CustomerContactId.name(), String.class));
        }
        return columns;
    }

    private OrphanContactExportParameters prepareInput(Object[][] accountData, Object[][] contactData,
            boolean entityMatchEnabled) {
        OrphanContactExportParameters parameters = new OrphanContactExportParameters();
        if (accountData != null) {
            uploadAvro(accountData, prepareAccountData(entityMatchEnabled), getAccountTable(entityMatchEnabled),
                    getAccountDir(entityMatchEnabled));
            parameters.setAccountTable(getAccountTable(entityMatchEnabled));
        }
        uploadAvro(contactData, prepareContactData(entityMatchEnabled), getContactTable(entityMatchEnabled),
                getContactDir(entityMatchEnabled));
        parameters.setContactTable(getContactTable(entityMatchEnabled));
        parameters.setValidatedColumns(Arrays.asList(ContactId.name(), ContactName.name(), AccountId.name(),
                CustomerContactId.name(), CustomerAccountId.name()));
        return parameters;
    }

    public void verifyResult(Object[][] expectedData, int expectNumOfRows) {
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get(ContactId.name()).toString(), expectedData[rowNum][0]);
            Assert.assertEquals(record.get(ContactName.name()).toString(), expectedData[rowNum][1]);
            Assert.assertEquals(record.get(AccountId.name()).toString(), expectedData[rowNum][2]);
            rowNum ++;
        }
        Assert.assertEquals(rowNum, expectNumOfRows);
    }

    private String getAccountTable(boolean entityMatchEnabled) {
        return entityMatchEnabled ? ACCOUNT_EM_TABLE : ACCOUNT_TABLE;
    }

    private String getContactTable(boolean entityMatchEnabled) {
        return entityMatchEnabled ? CONTACT_EM_TABLE : CONTACT_TABLE;
    }

    private String getAccountDir(boolean entityMatchEnabled) {
        return entityMatchEnabled ? ACCOUNT_EM_DIR : ACCOUNT_DIR;
    }

    private String getContactDir(boolean entityMatchEnabled) {
        return entityMatchEnabled ? CONTACT_EM_DIR : CONTACT_DIR;
    }
}
