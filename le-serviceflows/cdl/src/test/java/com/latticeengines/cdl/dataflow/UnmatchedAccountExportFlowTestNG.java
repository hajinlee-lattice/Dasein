package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.UnmatchedAccountExportParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class UnmatchedAccountExportFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    private static final String ACCOUNT_TABLE = "AccountTable";
    private static final String ACCOUNT_DIR = "/tmp/UnmatchedAccountExportFlowTestNG/account/";

    private Object[][] accountData = new Object[][] {
            // "AccountId", "Name", "LatticeAccountId"
            { "A001", "Husky", "Lattice_001" },
            { "A002", "Alaskan Malamute", "Lattice_002" },
            { "A003", "Collie", "Lattice_003" },
            { "A004", "Chihuahua", null },
            { "A005", "Labrador Retriever", "" },
            { "A006", "Corky", "Lattice_004" },
            { "A007", "Alaskan Cod", "" },
            { "A008", "Japanese Tofu", null },
            { "A009", "Corky", "Lattice_005" }
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
        UnmatchedAccountExportParameters parameters = prepareInput(accountData);
        executeDataFlow(parameters);
        verifyResult(expectedData);
    }

    @Override
    protected String getFlowBeanName() {
        return UnmatchedAccountExportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return ImmutableMap.of(ACCOUNT_TABLE, ACCOUNT_DIR + ACCOUNT_TABLE + ".avro");
    }

    private List<Pair<String, Class<?>>> prepareAccountData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Name.name(), String.class));
        columns.add(Pair.of(InterfaceName.LatticeAccountId.name(), String.class));
        return columns;
    }

    private UnmatchedAccountExportParameters prepareInput(Object[][] accountData) {
        UnmatchedAccountExportParameters parameters = new UnmatchedAccountExportParameters();
        uploadAvro(accountData, prepareAccountData(), ACCOUNT_TABLE, ACCOUNT_DIR);
        parameters.setAccountTable(ACCOUNT_TABLE);
        return parameters;
    }

    private void verifyResult(Object[][] expectedData) {
        List<GenericRecord> records = readOutput();
        int rowNum = 0;
        for (GenericRecord record : records) {
            Assert.assertEquals(record.get(InterfaceName.AccountId.name()).toString(), expectedData[rowNum][0]);
            Assert.assertEquals(record.get(InterfaceName.Name.name()).toString(), expectedData[rowNum][1]);
            if (record.get(InterfaceName.LatticeAccountId.name()) != null) {
                Assert.assertEquals(record.get(InterfaceName.LatticeAccountId.name()).toString(),
                        expectedData[rowNum][2]);
            } else {
                Assert.assertEquals(record.get(InterfaceName.LatticeAccountId.name()), expectedData[rowNum][2]);
            }
            rowNum++;
        }
        Assert.assertEquals(rowNum, 4);
    }
}
