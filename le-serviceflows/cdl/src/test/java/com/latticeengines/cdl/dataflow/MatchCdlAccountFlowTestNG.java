package com.latticeengines.cdl.dataflow;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.MatchCdlAccountParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class MatchCdlAccountFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchCdlAccountFlowTestNG.class);

    private MatchCdlAccountParameters getMatchAccountIdParameters() {
        MatchCdlAccountParameters params = new MatchCdlAccountParameters("inputTable", "accountTable");
        params.setInputMatchFields(Arrays.asList(InterfaceName.AccountId.name()));
        params.setAccountMatchFields(Arrays.asList(InterfaceName.AccountId.name()));
        params.setHasAccountId(true);
        return params;
    }

    private MatchCdlAccountParameters getMatchLatticeAccountIdParameters() {
        MatchCdlAccountParameters params = new MatchCdlAccountParameters("inputTable", "accountTable");
        params.setInputMatchFields(Arrays.asList(InterfaceName.LatticeAccountId.name()));
        params.setAccountMatchFields(Arrays.asList(InterfaceName.LatticeAccountId.name()));
        params.setHasAccountId(false);
        return params;
    }

    @Override
    protected String getFlowBeanName() {
        return "matchCdlAccountFlow";
    }

    @Test(groups = "functional")
    public void matchWithAccountId() throws Exception {
        executeDataFlow(getMatchAccountIdParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 50);
        GenericRecord record0 = null;
        GenericRecord record1 = null;

        for (GenericRecord record : outputRecords) {
            Object accountId = record.get("AccountId");
            if (accountId != null && accountId.toString().equals("1-10Q63")) {
                record0 = record;
                continue;
            }
            if (accountId != null && accountId.toString().equals("1-12UDF")) {
                record1 = record;
                continue;
            }
        }
        Assert.assertEquals(record0.get("PeriodId").toString(), "10");
        Assert.assertEquals(record1.get("LatticeAccountId").toString(), "39");
    }

    @Test(groups = "functional")
    public void matchWithLatticeAccountId() throws Exception {
        executeDataFlow(getMatchLatticeAccountIdParameters());
        List<GenericRecord> outputRecords = readOutput();
        assertEquals(outputRecords.size(), 50);

        GenericRecord record0 = null;
        GenericRecord record1 = null;

        for (GenericRecord record : outputRecords) {
            Object accountId = record.get("AccountId");
            if (accountId != null && accountId.toString().equals("1-10Q63")) {
                record0 = record;
                continue;
            }
            if (accountId != null && accountId.toString().equals("1-12UDF")) {
                record1 = record;
                continue;
            }
        }
        Assert.assertEquals(record0.get("PeriodId").toString(), "9");
        Assert.assertEquals(record1.get("PeriodId").toString(), "10");
    }

}
