package com.latticeengines.propdata.api.service.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;

public class MatchCommandServiceImplDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchCommandServiceImplDeploymentTestNG.class);

    @Autowired
    private MatchCommandService matchCommandService;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    @BeforeMethod(groups = "api.deployment")
    public void beforeMethod() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Test(groups = "api.deployment", dataProvider = "matchDataProvider", threadPoolSize = 3)
    public void testMatch(String sourceTable, String destTables, String contractId, MatchVerifier verifier) {
        MatchClientContextHolder.setMatchClient(getMatchClient()); // set match client for current thread.

        log.info("Match test with SourceTable=" + sourceTable + " DestTables="
                + destTables + " ContractID=" + contractId);

        CreateCommandRequest request = new CreateCommandRequest();
        request.setContractExternalID(contractId);
        request.setDestTables(destTables);
        request.setSourceTable(sourceTable);
        Commands command = matchCommandService.createMatchCommand(request);

        try {
            verifier.verify(command.getPid(), request);
        } finally {
            verifier.cleanupResultTales(command.getPid(), request);
        }
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
                { "DunsTest", "SWDuns_Source", "PD_Test", new DunsMatchVerifier() }
        };
    }

    // ==================================================
    // Verifiers
    // ==================================================
    private interface MatchVerifier {
        void verify(Long commandId, CreateCommandRequest request);
        void cleanupResultTales(Long commandId, CreateCommandRequest request);
    }

    private abstract class AbstractMatchVerifier implements MatchVerifier {
        @Override
        public void verify(Long commandId, CreateCommandRequest request) {
            verifyCreateCommandRequest(commandId, request);
            verifyResults(commandId, request);
        }

        @Override
        public void cleanupResultTales(Long commandId, CreateCommandRequest request) {
            String[] destTables = request.getDestTables().split("\\|");
            String commandName = request.getCommandType().getCommandName();
            Set<String> resultTables = new HashSet<>();
            for (String destTable: destTables) {
                String mangledTableName = String.format("%s_%s_%s",
                        commandName, String.valueOf(commandId), destTable);
                resultTables.add(mangledTableName);
            }
            for (String resultTable: resultTables) {
                tryDropTable(resultTable);
                tryDropTable(resultTable + "_MetaData");
            }
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

    private class DunsMatchVerifier extends AbstractMatchVerifier {
        @Override
        public void verifyResults(Long commandId, CreateCommandRequest request) {
            verifyResultTablesAreGenerated(commandId, 30);
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
            Assert.fail("Failed to wait for command being complete. CommandID=" + commandId, e);
            return MatchCommandStatus.UNKNOWN;
        }
    }

    private void verifyResultTablesAreGenerated(Long commandId, int timoutInMin) {
        MatchCommandStatus status = waitCommandComplete(timoutInMin, commandId);
        Assert.assertEquals(status, MatchCommandStatus.COMPLETE);

        Commands commands = matchCommandService.findMatchCommandById(commandId);
        Assert.assertTrue(waitResultTablesGenerated(commandId),
                "Matching against " + commands.getDestTables() + " failed to generate all the result tables. "
                        + " CommandId=" + commandId );
    }

    private boolean waitResultTablesGenerated(Long commandId) {
        Integer numRetries = 10;
        boolean ready;
        try {
            do {
                ready = matchCommandService.resultTablesAreReady(commandId);
                Thread.sleep(3000L);
            } while (!ready && numRetries-- > 0);
            return ready;
        } catch (InterruptedException e) {
            Assert.fail("Failed to wait for result tables being generate for CommandID=" + commandId, e);
            return false;
        }
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

    private boolean tableExists(String tableName) {
        String sql = String.format("IF OBJECT_ID (N'dbo.%s', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0", tableName);
        return jdbcTemplate.queryForObject(sql, Boolean.class);
    }

    private void tryDropTable(String tableName) {
        if (tableExists(tableName)) {
            jdbcTemplate.execute("DROP TABLE [PropDataMatchDB].[dbo].[" + tableName + "]");
        }
    }

}
