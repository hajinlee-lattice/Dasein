package com.latticeengines.propdata.match.service.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.propdata.match.datasource.MatchClientContextHolder;
import com.latticeengines.propdata.match.datasource.MatchClientRoutingDataSource;
import com.latticeengines.propdata.match.service.MatchCommandsService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

public class MatchAcceptanceServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchAcceptanceServiceImplTestNG.class);

    private static final String DerivedColumns = "DerivedColumns";

    @Autowired
    private MatchCommandsService matchCommandsService;

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private JdbcTemplate jdbcTemplate = new JdbcTemplate();

    @BeforeMethod(groups = "acceptance")
    public void beforeMethod() {
        jdbcTemplate.setDataSource(dataSource);
    }

    @Test(groups = "acceptance", dataProvider = "matchDataProvider", threadPoolSize = 3)
    public void testMatch(String sourceTable, String destTables, String contractId, MatchVerifier verifier) {
        MatchClientContextHolder.setMatchClient(getMatchClient()); // set match
                                                                   // client for
                                                                   // current
                                                                   // thread.

        log.info("Match test with SourceTable=" + sourceTable + " DestTables=" + destTables + " ContractID="
                + contractId);

        CreateCommandRequest request = new CreateCommandRequest();
        request.setContractExternalID(contractId);
        request.setDestTables(destTables);
        request.setSourceTable(sourceTable);
        Commands command = matchCommandsService.createMatchCommand(request);

        try {
            verifier.verify(command.getPid(), request);
        } finally {
            verifier.cleanupResultTales(command.getPid(), request);
        }
    }

    @DataProvider(name = "matchDataProvider", parallel = true)
    private Object[][] MatchDataProvider() {
        return new Object[][] {
                { "LocationData_1000_US", "DerivedColumns", "PD_Test", new DerivedColumnsLocationVerifier() } };
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
            for (String destTable : destTables) {
                String mangledTableName = String.format("%s_%s_%s", commandName, String.valueOf(commandId), destTable);
                resultTables.add(mangledTableName);
            }
            for (String resultTable : resultTables) {
                tryDropTable(resultTable);
                tryDropTable(resultTable + "_MetaData");
            }
        }

        abstract void verifyResults(Long commandId, CreateCommandRequest request);
    }

    private class DerivedColumnsLocationVerifier extends AbstractMatchVerifier {
        @Override
        public void verifyResults(Long commandId, CreateCommandRequest request) {
            verifyResultTablesAreGenerated(commandId, 30);

            verifyDerivedColumnsResultByRules(commandId);
        }
    }

    // ==================================================
    // verify methods
    // ==================================================
    private void verifyCreateCommandRequest(Long commandId, CreateCommandRequest request) {
        Commands command = matchCommandsService.findMatchCommandById(commandId);
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
                status = matchCommandsService.getMatchCommandStatus(commandId);
                Thread.sleep(5000L);
            } while (!status.equals(MatchCommandStatus.COMPLETE) && !status.equals(MatchCommandStatus.FAILED)
                    && numRetries-- > 0);
            return status;
        } catch (InterruptedException e) {
            Assert.fail("Failed to wait for command being complete. CommandID=" + commandId, e);
            return MatchCommandStatus.UNKNOWN;
        }
    }

    private void verifyResultTablesAreGenerated(Long commandId, int timoutInMin) {
        MatchCommandStatus status = waitCommandComplete(timoutInMin, commandId);
        Assert.assertEquals(status, MatchCommandStatus.COMPLETE);

        Commands commands = matchCommandsService.findMatchCommandById(commandId);
        Assert.assertTrue(waitResultTablesGenerated(commandId), "Matching against " + commands.getDestTables()
                + " failed to generate all the result tables. " + " CommandId=" + commandId);
    }

    private boolean waitResultTablesGenerated(Long commandId) {
        Integer numRetries = 10;
        boolean ready;
        try {
            do {
                ready = matchCommandsService.resultTablesAreReady(commandId);
                Thread.sleep(3000L);
            } while (!ready && numRetries-- > 0);
            return ready;
        } catch (InterruptedException e) {
            Assert.fail("Failed to wait for result tables being generate for CommandID=" + commandId, e);
            return false;
        }
    }

    private Boolean verifyDerivedColumnsResultByRules(Long commandId) {
        Commands command = matchCommandsService.findMatchCommandById(commandId);
        String processUid = command.getProcessUID();
        String sourceTable = command.getSourceTable();

        verifyResultsByRules(sourceTable, DerivedColumns, commandId, processUid);

        return verifyResultsByRules(sourceTable, DerivedColumns, commandId, processUid);
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

    private Boolean verifyResultsByRules(String testName, String targetTable, Long commandId, String processUID) {
        Boolean isPassed;
        isPassed = executeVerifyResultsByRulesSP(testName, targetTable, commandId, processUID);

        if (!isPassed) {
            findResultsByCommandID(commandId);
        }
        Assert.assertTrue(isPassed);
        return isPassed;
    }

    private Boolean executeVerifyResultsByRulesSP(final String testName, final String targetTable, final Long commandId,
            final String rootUID) {
        List<SqlParameter> declaredParameters = new ArrayList<>();
        final String tag = "location";
        declaredParameters.add(new SqlParameter("testName", Types.VARCHAR));
        declaredParameters.add(new SqlParameter("destTables", Types.VARCHAR));
        declaredParameters.add(new SqlParameter("tag", Types.VARCHAR));
        declaredParameters.add(new SqlParameter("commandId", Types.INTEGER));
        declaredParameters.add(new SqlParameter("taskUID", Types.VARCHAR));
        declaredParameters.add(new SqlOutParameter("isPassed", Types.BIT));

        Map<String, Object> resultsMap = jdbcTemplate.call(new CallableStatementCreator() {

            @Override
            public CallableStatement createCallableStatement(Connection con) throws SQLException {
                CallableStatement stmnt;
                stmnt = con.prepareCall(
                        "{call [PropDataMatchDB].[dbo].[PropDataTest_verifyResultByRule](?, ?, ?, ?, ?, ?)}");
                stmnt.setString("testName", testName);
                stmnt.setString("destTables", targetTable);
                stmnt.setString("tag", tag);
                stmnt.setLong("commandId", commandId);
                stmnt.setString("taskUID", rootUID);
                stmnt.registerOutParameter("isPassed", Types.BIT);
                return stmnt;
            }
        }, declaredParameters);
        return (Boolean)resultsMap.get("isPassed");
    }

    private void findResultsByCommandID(final Long commandId) {
        Object[] parameters = new Object[] { commandId };
        List results = jdbcTemplate.queryForList(
                "SELECT * FROM [PropDataMatchDB].[dbo].[DataValidator_ViolationTable] where CommandID = ?", parameters);
        log.error("The violation results are:");
        for (Object result : results) {
            log.error(result.toString());
        }
    }

}
