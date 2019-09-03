package com.latticeengines.cdl.dataflow;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class UnmatchedAccountExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    private static final String ACCOUNT_TABLE = "AccountTable";
    private static final String ACCOUNT_EM_TABLE = "AccountEMTable";
    private static final String ACCOUNT_DIR = "/tmp/UnmatchedAccountExportFlowTestNG/account/legacy/";
    private static final String ACCOUNT_EM_DIR = "/tmp/UnmatchedAccountExportFlowTestNG/account/em/";

    private Object[][] accountData = new Object[][] {
            // "AccountId", "Name", "LatticeAccountId"
            { "A001", "Husky", "Lattice_001" }, //
            { "A002", "Alaskan Malamute", "Lattice_002" }, //
            { "A003", "Collie", "Lattice_003" }, //
            { "A004", "Chihuahua", null }, //
            { "A005", "Labrador Retriever", "" }, //
            { "A006", "Corky", "Lattice_004" }, //
            { "A007", "Alaskan Cod", "" }, //
            { "A008", "Japanese Tofu", null }, //
            { "A009", "Corky", "Lattice_005" } //
    };

    private Object[][] accountEMData = new Object[][] {
            // "AccountId", "Name", "LatticeAccountId", "CustomerAccountId"
            { "A001", "Husky", "Lattice_001", "CA001" }, //
            { "A002", "Alaskan Malamute", "Lattice_002", "CA002" }, //
            { "A003", "Collie", "Lattice_003", "CA003" }, //
            { "A004", "Chihuahua", null, "CA004" }, //
            { "A005", "Labrador Retriever", "", "CA005" }, //
            { "A006", "Corky", "Lattice_004", "CA006" }, //
            { "A007", "Alaskan Cod", "", "CA007" }, //
            { "A008", "Japanese Tofu", null, "CA008" }, //
            { "A009", "Corky", "Lattice_005", "CA009" } //
    };

    private Object[][] expectedData = new Object[][] {
            // "AccountId", "Name", "LatticeAccountId"
            { "A004", "Chihuahua", null },
            { "A005", "Labrador Retriever", "" },
            { "A007", "Alaskan Cod", "" },
            { "A008", "Japanese Tofu", null }
    };

    @Test(groups = "functional")
    public void testUnmatchedAccountExportFlow() {
        UnmatchedAccountExportParameters parameters = prepareInput(accountData, false);
        executeDataFlow(parameters);
        verifyResult(expectedData);
    }

    @Test(groups = "functional")
    public void testUnmatchedAccountEMExportFlow() {
        UnmatchedAccountExportParameters parameters = prepareInput(accountEMData, true);
        executeDataFlow(parameters);
        verifyResult(expectedData);
    }

    @Override
    protected String getFlowBeanName() {
        return UnmatchedAccountExportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of( //
                ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro", //
                ACCOUNT_EM_TABLE, ACCOUNT_EM_DIR + ACCOUNT_EM_TABLE + ".avro" //
        );
    }

    private List<Pair<String, Class<?>>> prepareAccountData(boolean entityMatchEnabled) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(AccountId.name(), String.class));
        columns.add(Pair.of(Name.name(), String.class));
        columns.add(Pair.of(LatticeAccountId.name(), String.class));
        if (entityMatchEnabled) {
            columns.add(Pair.of(CustomerAccountId.name(), String.class));
        }
        return columns;
    }

    private UnmatchedAccountExportParameters prepareInput(Object[][] accountData, boolean entityMatchEnabled) {
        UnmatchedAccountExportParameters parameters = new UnmatchedAccountExportParameters();
        uploadAvro(accountData, prepareAccountData(entityMatchEnabled), getAccountTable(entityMatchEnabled),
                getAccountDir(entityMatchEnabled));
        parameters.setAccountTable(getAccountTable(entityMatchEnabled));
        parameters.setValidatedColumns(
                Arrays.asList(AccountId.name(), Name.name(), LatticeAccountId.name(), CustomerAccountId.name()));
        return parameters;
    }

    private void verifyResult(Object[][] expectedData) {
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get(AccountId.name()).toString(), expectedData[rowNum][0]);
            Assert.assertEquals(record.get(Name.name()).toString(), expectedData[rowNum][1]);
            if (record.get(LatticeAccountId.name()) != null) {
                Assert.assertEquals(record.get(LatticeAccountId.name()).toString(),
                        expectedData[rowNum][2]);
            } else {
                Assert.assertEquals(record.get(LatticeAccountId.name()), expectedData[rowNum][2]);
            }
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }

    private String getAccountTable(boolean entityMatchEnabled) {
        return entityMatchEnabled ? ACCOUNT_EM_TABLE : ACCOUNT_TABLE;
    }

    private String getAccountDir(boolean entityMatchEnabled) {
        return entityMatchEnabled ? ACCOUNT_EM_DIR : ACCOUNT_DIR;
    }
}
