package com.latticeengines.serviceflows.workflow.match;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("createEventTableFromMatchResult")
public class CreateEventTableFromMatchResult extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Log log = LogFactory.getLog(CreateEventTableFromMatchResult.class);

    @Override
    public void execute() {
        log.info("Inside CreateEventTableFromMatchResult execute()");

        Long matchCommandId = executionContext.getLong(MATCH_COMMAND_ID);
        Table preMatchEventTable = JsonUtils.deserialize(executionContext.getString(PREMATCH_EVENT_TABLE), Table.class);
        DbCreds dbCreds = JsonUtils.deserialize(executionContext.getString(DB_CREDS), DbCreds.class);

        Table eventTable = null;
        try {
            eventTable = createEventTableFromMatchResult(matchCommandId, preMatchEventTable, dbCreds);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28005, e, new String[] { String.valueOf(matchCommandId) });
        } finally {
            try {
                boolean deleted = deleteEventTableFromMatchDB(preMatchEventTable, dbCreds);
                if (!deleted) {
                    log.warn("Table " + preMatchEventTable.getName() + " was not dropped from the PD match db.");
                }
            } catch (Exception e) {
                log.error(e);
            }
        }

        executionContext.putString(EVENT_TABLE, JsonUtils.serialize(eventTable));
        executionContext.putString(MATCH_TABLE, eventTable.getName());
    }

    private boolean deleteEventTableFromMatchDB(Table preMatchEventTable, DbCreds dbCreds) throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        try (Connection conn = DriverManager.getConnection(dbCreds.getJdbcUrl())) {
            try (PreparedStatement pstmt = conn.prepareStatement("DROP TABLE " + preMatchEventTable.getName())) {
                return pstmt.execute();
            }
        }
    }

    private Table createEventTableFromMatchResult(Long commandId, //
            Table preMatchEventTable, DbCreds dbCreds) throws Exception {
        Table table = preMatchEventTable;

        Attribute idColumn = getIdColumn(table);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT Source_" + idColumn.getName());
        for (Attribute attr : table.getAttributes()) {
            sb.append(", Source_" + attr.getName() + " AS " + attr.getName()).append("\n");
        }

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String matchTableName = configuration.getMatchCommandType().getCommandName() + "_" + commandId + "_"
                + configuration.getDestTables();
        log.info("matchTableName:" + matchTableName);
        try (Connection conn = DriverManager.getConnection(dbCreds.getJdbcUrl())) {
            String query = "SELECT DISTINCT s.name FROM SYSOBJECTS, SYSCOLUMNS s, " + matchTableName + "_Metadata r"
                    + " WHERE SYSOBJECTS.id = s.id AND " + " SYSOBJECTS.xtype = 'u' AND " + " SYSOBJECTS.name = '"
                    + matchTableName + "'" + " AND r.InternalColumnName = s.name";

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                ResultSet rset = pstmt.executeQuery();

                while (rset.next()) {
                    String column = rset.getString(1);
                    sb.append(", ").append(column).append("\n");
                }
            }
            sb.append(" FROM " + matchTableName + " WHERE $CONDITIONS");
        }
        String hdfsTargetPath = getTargetPath() + "/" + matchTableName;
        String url = String.format("%s/modeling/dataloads", configuration.getMicroServiceHostPort());
        LoadConfiguration config = new LoadConfiguration();
        config.setCreds(dbCreds);
        config.setQuery(sb.toString());
        config.setCustomer(configuration.getCustomerSpace().toString());
        config.setKeyCols(Arrays.<String> asList(new String[] { "Source_" + idColumn.getName() }));
        config.setTargetHdfsDir(hdfsTargetPath);

        AppSubmission submission = restTemplate.postForObject(url, config, AppSubmission.class);
        waitForAppId(submission.getApplicationIds().get(0).toString(), configuration.getMicroServiceHostPort());
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, hdfsTargetPath, null, null);
        eventTable.setName(matchTableName);

        addMetadata(eventTable, dbCreds);
        addMetadataFromPreMatchTable(eventTable, preMatchEventTable);
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", configuration.getMicroServiceHostPort(),
                configuration.getCustomerSpace(), eventTable.getName());
        restTemplate.postForLocation(url, eventTable);
        return eventTable;
    }

    private Attribute getIdColumn(Table table) {
        List<Attribute> idColumns = table.getAttributes(LogicalDataType.InternalId);
        if (idColumns.isEmpty()) {
            if (table.getAttribute("Id") == null) {
                throw new RuntimeException("No Id columns found in prematch table");
            } else {
                log.warn("No column with LogicalDataType InternalId in prematch table.  Choosing column called \"Id\"");
                idColumns.add(table.getAttribute("Id"));
            }
        }
        if (idColumns.size() != 1) {
            log.warn(String.format("Multiple id columns in prematch table.  Choosing %s", idColumns.get(0).getName()));
        }
        return idColumns.get(0);
    }

    private String getTargetPath() {
        CustomerSpace space = configuration.getCustomerSpace();
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }

    private void addMetadata(Table table, DbCreds creds) throws Exception {
        try (Connection conn = DriverManager.getConnection(creds.getJdbcUrl())) {
            String query = "SELECT InternalColumnName, MetaDataName, MetaDataValue FROM " + table.getName()
                    + "_MetaData ORDER BY InternalColumnName";
            Map<String, Attribute> map = table.getNameAttributeMap();
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                ResultSet rset = pstmt.executeQuery();

                while (rset.next()) {
                    String column = rset.getString(1);
                    String metadataName = rset.getString(2);
                    String metadataValue = rset.getString(3);

                    if (metadataValue != null) {
                        metadataValue = metadataValue.trim();
                    }

                    Attribute attr = map.get(column);

                    if (attr == null) {
                        continue;
                    }
                    AttributeUtils.setPropertyFromString(attr, metadataName, metadataValue);
                }
            }
        }
    }

    private void addMetadataFromPreMatchTable(Table eventTable, Table preMatchEventTable) {
        for (Attribute preMatchAttribute : preMatchEventTable.getAttributes()) {
            Attribute postMatchAttribute = eventTable.getAttribute(preMatchAttribute.getName());
            if (postMatchAttribute != null) {
                AttributeUtils.mergeAttributes(preMatchAttribute, postMatchAttribute);
            }
        }
    }
}
