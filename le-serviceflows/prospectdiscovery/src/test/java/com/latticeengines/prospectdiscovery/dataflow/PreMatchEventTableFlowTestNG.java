package com.latticeengines.prospectdiscovery.dataflow;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class PreMatchEventTableFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void execute() throws Exception {
        Table result = executeDataFlow();

        Assert.assertEquals(result.getExtracts().size(), 1);
        Assert.assertTrue(result.getAttributes().size() > 0);

        List<GenericRecord> outputTable = readTable(result.getExtracts().get(0).getPath() + "/*.avro");
        List<GenericRecord> accountTable = readTable(sourcePaths.get("Account"));

        Assert.assertTrue(identicalSets(outputTable, "Id", accountTable, "Id"));

        // Confirm that email stop-list filtering worked
        for (GenericRecord record : outputTable) {
            Assert.assertTrue(record.get("Domain") == null || !record.equals("gmail.com"));
        }

        Table schema = getOutputSchema();
        List<String> fields = Arrays.asList(schema.getAttributeNames());
        Assert.assertTrue(fields.contains("Domain"));
        Assert.assertTrue(fields.contains("City"));
        Assert.assertTrue(fields.contains("Street"));
        Assert.assertTrue(fields.contains("State"));
        Assert.assertTrue(fields.contains("Country"));
        Assert.assertTrue(fields.contains("PostalCode"));
    }

    @Override
    public String getFlowBeanName() {
        return "preMatchEventTableFlow";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        if (tableName.equals("PublicDomain")) {
            return null;
        }
        return "Id";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        if (tableName.equals("Account")) {
            return "CreatedDate";
        } else if (tableName.equals("PublicDomain")) {
            return null;
        }
        return "LastModifiedDate";
    }
}
