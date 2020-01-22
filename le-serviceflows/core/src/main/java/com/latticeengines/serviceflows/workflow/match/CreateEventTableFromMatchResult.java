package com.latticeengines.serviceflows.workflow.match;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("createEventTableFromMatchResult")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CreateEventTableFromMatchResult extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CreateEventTableFromMatchResult.class);

    @Inject
    private ModelProxy modelProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside CreateEventTableFromMatchResult execute()");

        Long matchCommandId = getLongValueFromContext(MATCH_COMMAND_ID);
        Table preMatchEventTable = getObjectFromContext(PREMATCH_EVENT_TABLE, Table.class);
        DbCreds dbCreds = getObjectFromContext(DB_CREDS, DbCreds.class);

        Table eventTable = null;
        try {
            eventTable = createEventTableFromMatchResult(matchCommandId, preMatchEventTable, dbCreds);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_28005, e, new String[] { String.valueOf(matchCommandId) });
        } finally {
            try {
                if (!configuration.isRetainMatchTables()) {
                    boolean deleted = deleteEventTableFromMatchDB(preMatchEventTable, dbCreds);
                    if (!deleted) {
                        log.warn("Table " + preMatchEventTable.getName() + " was not dropped from the PD match db.");
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        putObjectInContext(EVENT_TABLE, eventTable);
        putStringValueInContext(MATCH_TABLE, eventTable.getName());
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
        LoadConfiguration config = new LoadConfiguration();
        config.setCreds(dbCreds);
        config.setQuery(sb.toString());
        config.setCustomer(configuration.getCustomerSpace().toString());
        config.setKeyCols(Arrays.<String> asList(new String[] { "Source_" + idColumn.getName() }));
        config.setTargetHdfsDir(hdfsTargetPath);
        config.setTable(matchTableName);

        AppSubmission submission = modelProxy.loadData(config);
        waitForAppId(submission.getApplicationIds().get(0));
        Table eventTable = MetadataConverter.getTable(yarnConfiguration, hdfsTargetPath, null, null);
        eventTable.setName(matchTableName);

        addMetadata(eventTable, dbCreds);
        addMetadataFromPreMatchTable(eventTable, preMatchEventTable);

        metadataProxy.createTable(configuration.getCustomerSpace().toString(), eventTable.getName(), eventTable);

        if (!configuration.isRetainMatchTables()) {
            deleteMatchTable(dbCreds, matchTableName);
        }

        return eventTable;
    }

    private void deleteMatchTable(DbCreds dbCreds, String matchTableName) throws Exception {
        Connection conn = DriverManager.getConnection(dbCreds.getJdbcUrl());
        PreparedStatement pstmt = conn.prepareStatement(String.format("DROP TABLE %s", matchTableName));
        pstmt.execute();

        pstmt = conn.prepareStatement(String.format("DROP TABLE %s_MetaData", matchTableName));
        pstmt.execute();
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
                AttributeUtils.copyPropertiesFromAttribute(preMatchAttribute, postMatchAttribute, false);
            }
        }
    }
}
