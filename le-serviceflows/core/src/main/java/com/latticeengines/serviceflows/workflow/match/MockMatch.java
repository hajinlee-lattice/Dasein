package com.latticeengines.serviceflows.workflow.match;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("mockMatch")
public class MockMatch extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(MockMatch.class);
    private static final String DUPLICATE_TABLE_STRING_FORMAT = "IF OBJECT_ID('%s', 'U') IS NULL"
            + " BEGIN Select * into %s from %s END";
    private static final String SOURCE_TABLE_NAME = "PDEndToEnd_DerivedColumns";
    private static final String DESTINATION_TABLE_NAME = "RunMatchWithLEUniverse_123_DerivedColumns";
    private static final String SAMPLE_BASE_DIR = "/user/s-analytics/customers/%s/data/%s/samples";
    private static final Long MATCH_COMMAND_ID_NUMBER = 123L;

    private JdbcTemplate jdbcTemplate;

    @Override
    public void execute() {
        log.info("Inside MockMatch execute()");

        DbCreds dbCreds = JsonUtils.deserialize(executionContext.getString(DB_CREDS), DbCreds.class);
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

           try {
               Connection connection = DriverManager.getConnection(dbCreds.getJdbcUrl());

               log.info("Copying RunMatchWithLEUniverse_123_DerivedColumns from PDEndToEnd_DerivedColumns if it has been wiped.");
               PreparedStatement copyTables = connection.prepareStatement(String.format(DUPLICATE_TABLE_STRING_FORMAT,
                       DESTINATION_TABLE_NAME, DESTINATION_TABLE_NAME, SOURCE_TABLE_NAME));
               copyTables.execute();

               log.info("Copying RunMatchWithLEUniverse_123_DerivedColumns_Metadata from PDEndToEnd_DerivedColumns_Metadata if it has been wiped.");
               copyTables = connection.prepareStatement(String.format(DUPLICATE_TABLE_STRING_FORMAT,
                       DESTINATION_TABLE_NAME+"_Metadata", DESTINATION_TABLE_NAME+"_Metadata", SOURCE_TABLE_NAME+"_Metadata"));
               copyTables.execute();
               connection.commit();

               executionContext.putLong(MATCH_COMMAND_ID, MATCH_COMMAND_ID_NUMBER);
               ensureHDFSFilesAreDeleted();
           } catch (SQLException exp) {
               log.warn(String.format("Exception opening a connection with jdbcUrl: %s", dbCreds.getJdbcUrl()), exp);
           }
        } catch (ClassNotFoundException e) {
            log.warn("Class not found: com.microsoft.sqlserver.jdbc.SQLServerDriver", e);
        }
    }

    private void ensureHDFSFilesAreDeleted() {
        String targetHdfsPathForMatchFiles = getTargetPath() + "/" + DESTINATION_TABLE_NAME;
        String targetHdfsPathForSampleFiles = String.format(SAMPLE_BASE_DIR, configuration.getCustomerSpace().toString(), DESTINATION_TABLE_NAME);

        try {
            FileSystem fs = FileSystem.get(yarnConfiguration);
            fs.delete(new Path(targetHdfsPathForMatchFiles), true);
            fs.delete(new Path(targetHdfsPathForSampleFiles), true);
        } catch (IOException e) {
            log.warn(String.format("Cannot delete file path from HDFS: %s", targetHdfsPathForMatchFiles), e);
        }
    }

    private String getTargetPath() {
        CustomerSpace space = configuration.getCustomerSpace();
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }

}
