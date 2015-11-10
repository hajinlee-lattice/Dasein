package com.latticeengines.workflowapi.steps.prospectdiscovery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.AbstractMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;

@Component("loadHdfsTableToPDServer")
public class LoadHdfsTableToPDServer extends BaseFitModelStep<BaseFitModelStepConfiguration> {

    private static final Log log = LogFactory.getLog(LoadHdfsTableToPDServer.class);

    @Override
    public void execute() {
        log.info("Inside LoadHdfsTableToPDServer execute()");

        AbstractMap.SimpleEntry<Table, DbCreds> preMatchEventTableAndCreds = loadHdfsTableToPDServer();

        executionContext.putString(PREMATCH_EVENT_TABLE, JsonUtils.serialize(preMatchEventTableAndCreds.getKey()));
        executionContext.putString(DB_CREDS, JsonUtils.serialize(preMatchEventTableAndCreds.getValue()));
    }

    private AbstractMap.SimpleEntry<Table, DbCreds> loadHdfsTableToPDServer() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace(), "PrematchFlow");
        Table prematchFlowTable = restTemplate.getForObject(url, Table.class);

        DbCreds.Builder credsBuilder = new DbCreds.Builder()
                . //
                dbType("SQLServer")
                . //
                jdbcUrl("jdbc:sqlserver://10.51.15.130:1433;databaseName=PropDataMatchDB;user=DLTransfer;password=free&NSE")
                . //
                user("DLTransfer"). //
                password("free&NSE");
        DbCreds creds = new DbCreds(credsBuilder);

        createTable(prematchFlowTable, creds);

        exportConfig.setTable(prematchFlowTable.getName());
        exportConfig.setCustomer(configuration.getCustomerSpace());
        exportConfig.setHdfsDirPath(prematchFlowTable.getExtracts().get(0).getPath());
        exportConfig.setCreds(creds);

        url = String.format("%s/modeling/dataexports", configuration.getMicroServiceHostPort());
        AppSubmission submission = restTemplate.postForObject(url, exportConfig, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());

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
        switch (type) {
        case "double":
            return "FLOAT";
        case "float":
            return "FLOAT";
        case "string":
            return "VARCHAR(255)";
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
