package com.latticeengines.serviceflows.workflow.match;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.AbstractMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("loadHdfsTableToPDServer")
public class LoadHdfsTableToPDServer extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(LoadHdfsTableToPDServer.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelProxy modelProxy;

    @Override
    public void execute() {
        log.info("Inside LoadHdfsTableToPDServer execute()");

        AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds = loadHdfsTableToPDServer();

        executionContext.putString(PREMATCH_EVENT_TABLE, JsonUtils.serialize(preMatchEventTableAndCreds.getKey()));
        executionContext.putString(DB_CREDS, JsonUtils.serialize(preMatchEventTableAndCreds.getValue()));
    }

    private AbstractMap.SimpleEntry<Table, DbCreds> loadHdfsTableToPDServer() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        Table prematchFlowTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getInputTableName());
        prematchFlowTable.setName(prematchFlowTable.getName() + "_" + System.currentTimeMillis());

        String jdbcUrl = configuration.getDbUrl();
        String password = CipherUtils.decrypt(configuration.getDbPasswordEncrypted());
        jdbcUrl = jdbcUrl.replaceAll("\\$\\$USER\\$\\$", configuration.getDbUser());
        jdbcUrl = jdbcUrl.replaceAll("\\$\\$PASSWD\\$\\$", password);

        // SQLServer is the only supported match dbtype
        @SuppressWarnings("deprecation")
        DbCreds.Builder credsBuilder = new DbCreds.Builder() //
                .dbType("SQLServer") //
                .jdbcUrl(jdbcUrl) //
                .user(configuration.getDbUser()) //
                .password(password);
        DbCreds creds = new DbCreds(credsBuilder);

        createTable(prematchFlowTable, creds);

        exportConfig.setTable(prematchFlowTable.getName());
        exportConfig.setCustomer(configuration.getCustomerSpace().toString());
        String path = ExtractUtils.getSingleExtractPath(yarnConfiguration, prematchFlowTable);
        exportConfig.setHdfsDirPath(path);
        exportConfig.setCreds(creds);

        AppSubmission submission = modelProxy.exportData(exportConfig);
        waitForAppId(submission.getApplicationIds().get(0), configuration.getMicroServiceHostPort());

        return new AbstractMap.SimpleEntry<Table, DbCreds>(prematchFlowTable, creds);
    }

    private void createTable(Table table, DbCreds creds) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s (\n", table.getName()));
        int size = table.getAttributes().size();
        int i = 1;
        for (Attribute attr : table.getAttributes()) {
            sb.append(String.format("  %s %s%s\n", attr.getName(), getSQLServerType(attr.getPhysicalDataType()),
                    i == size ? ")" : ","));
            i++;
        }

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl())) {
                try (PreparedStatement dropTablePstmt = conn.prepareStatement("DROP TABLE " + table.getName())) {
                    try {
                        dropTablePstmt.executeUpdate();
                    } catch (Exception e) {
                        // ignore
                    }
                }

                try (PreparedStatement createTablePstmt = conn.prepareStatement(sb.toString())) {
                    createTablePstmt.executeUpdate();
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28004, e, new String[] { table.getName() });
        }
    }

    private String getSQLServerType(String type) {
        String lower = type.toLowerCase();
        switch (lower) {
        case "double":
            return "FLOAT";
        case "float":
            return "FLOAT";
        case "string":
            return "VARCHAR(MAX)";
        case "long":
            return "BIGINT";
        case "boolean":
            return "BIT";
        case "int":
            return "INT";
        default:
            throw new RuntimeException("Unknown SQL Server type for avro type " + type);
        }
    }

}
