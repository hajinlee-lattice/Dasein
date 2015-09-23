package com.latticeengines.propdata.api.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.propdata.api.datasource.MatchClientContextHolder;
import com.latticeengines.propdata.api.datasource.MatchClientRoutingDataSource;
import com.latticeengines.propdata.api.service.MatchCommandService;
import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

public class MatchCommandServiceImplTestNG extends PropDataApiFunctionalTestNGBase {

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    @BeforeMethod(groups = "api.functional")
    public void beforeMethod() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Test(groups = "api.functional", dataProvider = "matchDataProvider", threadPoolSize = 3)
    public void testMatch(String sourceTable, String destTables, String contractId, MatchVerifier verifier) {
        MatchClientContextHolder.setMatchClient(getMatchClient()); // set match client for current thread.

        CreateCommandRequest request = new CreateCommandRequest();
        request.setContractExternalID(contractId);
        request.setDestTables(destTables);
        request.setSourceTable(sourceTable);
        Commands command = matchCommandService.createMatchCommand(request);

        verifier.verify(command.getPid(), request);
    }

    @DataProvider(name = "matchDataProvider", parallel = true)
    private Object[][] MatchDataProvider() {
        return new Object[][] {
                { "Fortune1000", "Alexa_Source|Experian_Source|DerivedColumns", "PD_Test", new Fortune1000Verifier() },
                { "PayPal_matching_elements_small",
                    "HGData_Source|Alexa_Source|BuiltWith_Source|Semrush|LexisNexis_Source|OrbIntelligence_Source|Experian_Source|DerivedColumns",
                        "PD_Test", new NonEmptyMatchVerifier() },
                { "PayPal_Empty", "Alexa_Source|DerivedColumns", "PD_Test", new EmptyMatchVerifier() },
                { "DomainIdEmpty_Test", "Alexa_Source|DerivedColumns", "PD_Test", new EmptyMatchVerifier() },
                { "LocationIdEmpty_Test", "Alexa_Source|DerivedColumns", "PD_Test", new EmptyMatchVerifier() },
                { "Duns_Test", "SWDuns_Source", "PD_Test", new NonEmptyMatchVerifier() }
        };
    }

    // ==================================================
    // Verifiers
    // ==================================================
    private interface MatchVerifier { void verify(Long commandId, CreateCommandRequest request); }

    private abstract class AbstractMatchVerifier implements MatchVerifier {
        @Override
        public void verify(Long commandId, CreateCommandRequest request) {
            verifyCreateCommandRequest(commandId, request);
            verifyResults(commandId, request);
        }

        abstract void verifyResults(Long commandId, CreateCommandRequest request);
    }

    private class EmptyMatchVerifier extends AbstractMatchVerifier {
        @Override
        public void verifyResults(Long commandId, CreateCommandRequest request) {
            verifyResultTablesAreGenerated(commandId, 5);

            Collection<String> resultTables = matchCommandService.generatedResultTables(commandId);
            for (String restultTable: resultTables) {
                verifyEmptyResultTable(restultTable);
            }
        }

    }

    private class NonEmptyMatchVerifier extends AbstractMatchVerifier {
        @Override
        public void verifyResults(Long commandId, CreateCommandRequest request) {
            verifyResultTablesAreGenerated(commandId, 30);
            verifyDerivedColumnsResultNonEmpty(commandId);
        }
    }

    private class Fortune1000Verifier extends NonEmptyMatchVerifier {
        @Override
        public void verifyResults(Long commandId, CreateCommandRequest request) {
            verifyResultTablesAreGenerated(commandId, 30);
            String resultTable = verifyDerivedColumnsResultNonEmpty(commandId);
            verifyMinimumMatchedAccounts(resultTable, 700);
        }
    }

    // ==================================================
    // verify methods
    // ==================================================
    private void verifyCreateCommandRequest(Long commandId, CreateCommandRequest request) {
        Commands command = matchCommandService.findMatchCommandById(commandId);
        Assert.assertEquals(command.getContractExternalID(), request.getContractExternalID());
        Assert.assertEquals(command.getDeploymentExternalID(), request.getContractExternalID());
        Assert.assertEquals(command.getCommandName(), MatchCommandType.MATCH_WITH_UNIVERSE.getCommandName());
        Assert.assertEquals(command.getDestTables(), request.getDestTables());
    }

    private MatchCommandStatus waitCommandComplete(Integer timeOutInMinutes, Long commandId) {
        Integer numRetries = timeOutInMinutes * 12;
        MatchCommandStatus status;
        try {
            do {
                status = matchCommandService.getMatchCommandStatus(commandId);
                Thread.sleep(5000L);
            } while (!status.equals(MatchCommandStatus.COMPLETE) &&
                    !status.equals(MatchCommandStatus.FAILED) && numRetries-- > 0);
            return status;
        } catch (InterruptedException e) {
            Assert.fail("Failed to wait for command being complete.", e);
            return MatchCommandStatus.UNKNOWN;
        }
    }

    private void verifyResultTablesAreGenerated(Long commandId, int timoutInMin) {
        Assert.assertFalse(matchCommandService.resultTablesAreReady(commandId));

        MatchCommandStatus status = waitCommandComplete(timoutInMin, commandId);
        Assert.assertEquals(status, MatchCommandStatus.COMPLETE);

        Assert.assertTrue(matchCommandService.resultTablesAreReady(commandId));
    }

    private String verifyDerivedColumnsResultNonEmpty(Long commandId) {
        String resultTable = getDerivedColumnsResultTableName(commandId);
        Assert.assertNotNull(resultTable, "Should have a result table for derived columns");
        verifyNonEmptyResultTable(resultTable);
        return resultTable;
    }

    private void verifyEmptyResultTable(String tableName) {
        List<Map<String, Object>> result =
                jdbcTemplate.queryForList("SELECT * FROM [PropDataMatchDB].[dbo].[" + tableName + "]");
        Assert.assertTrue(result.isEmpty(), "Result table should be empty.");
    }

    private void verifyNonEmptyResultTable(String tableName) {
        List<Map<String, Object>> result =
                jdbcTemplate.queryForList("SELECT * FROM [PropDataMatchDB].[dbo].[" + tableName + "]");
        Assert.assertFalse(result.isEmpty(), "Result table should not be empty.");
    }

    private String getDerivedColumnsResultTableName(Long commandId) {
        Collection<String> resultTables = matchCommandService.generatedResultTables(commandId);
        for (String restultTable: resultTables) {
            if (restultTable.contains("DerivedColumns")) {
                return restultTable;
            }
        }
        return null;
    }

    private void verifyMinimumMatchedAccounts(String tableName, int minAccounts) {
        List<Map<String, Object>> result =
                jdbcTemplate.queryForList("SELECT * FROM [PropDataMatchDB].[dbo].[" + tableName + "]");
        Assert.assertTrue(result.size() >= minAccounts, tableName + " should have at least " + minAccounts + " rows.");
    }

}
