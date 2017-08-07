package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.etl.publication.metadata.SQLServerMetadataProvider;
import com.latticeengines.datacloud.etl.publication.service.PublishConfigurationParser;
import com.latticeengines.datacloud.etl.service.SourceColumnService;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.publication.PublishTextToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.SqlDestination;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("publishConfigurationParser")
public class PublishConfigurationParserImpl implements PublishConfigurationParser {

    private static final String JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION = "-Dexport.statements.per.transaction=1";
    private static final String JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT = "-Dsqoop.export.records.per.statement=1000";
    private static final String SQLSERVER_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String STAGE_SUFFIX = "_Stage";
    private static final String BACKUP_SUFFIX = "_Bak";

    @Value("${datacloud.collection.host}")
    private String collectionHost;

    @Value("${datacloud.collection.port}")
    private int collectionPort;

    @Value("${datacloud.collection.db}")
    private String collectionDb;

    @Value("${datacloud.bulk.host}")
    private String bulkHost;

    @Value("${datacloud.bulk.port}")
    private int bulkPort;

    @Value("${datacloud.bulk.db}")
    private String bulkDb;

    @Value("${datacloud.test.host}")
    private String testHost;

    @Value("${datacloud.test.port}")
    private int testPort;

    @Value("${datacloud.test.db}")
    private String testDb;

    @Value("${datacloud.user}")
    private String dbUser;

    @Value("${datacloud.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.collection.sqoop.mapper.number}")
    private int numMappers;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private SourceColumnService sourceColumnService;

    @Override
    public <T extends PublishToSqlConfiguration> T parseSqlAlias(T sqlConfiguration) {
        PublishToSqlConfiguration.Alias alias = sqlConfiguration.getAlias();
        if (alias != null) {
            switch (alias) {
            case CollectionDB:
                sqlConfiguration.setHost(collectionHost);
                sqlConfiguration.setPort(collectionPort);
                sqlConfiguration.setDatabase(collectionDb);
                sqlConfiguration.setUsername(dbUser);
                sqlConfiguration.setEncryptedPassword(CipherUtils.encrypt(dbPassword));
                break;
            case BulkDB:
                sqlConfiguration.setHost(bulkHost);
                sqlConfiguration.setPort(bulkPort);
                sqlConfiguration.setDatabase(bulkDb);
                sqlConfiguration.setUsername(dbUser);
                sqlConfiguration.setEncryptedPassword(CipherUtils.encrypt(dbPassword));
                break;
            case TestDB:
                sqlConfiguration.setHost(testHost);
                sqlConfiguration.setPort(testPort);
                sqlConfiguration.setDatabase(testDb);
                sqlConfiguration.setUsername(dbUser);
                sqlConfiguration.setEncryptedPassword(CipherUtils.encrypt(dbPassword));
                break;
            case SourceDB:
            default:
                    break;
            }
        }
        return sqlConfiguration;
    }

    @Override
    public SqoopExporter constructSqoopExporter(PublishToSqlConfiguration sqlConfiguration, String avroDir) {
        SqlDestination destination = (SqlDestination) sqlConfiguration.getDestination();
        String tableName = destination.getTableName();
        switch (sqlConfiguration.getPublicationStrategy()) {
            case VERSIONED:
            case REPLACE:
                tableName = tableName + STAGE_SUFFIX;
                break;
            case APPEND:
                break;
        }
        String customer = String.format(EngineConstants.SQOOP_CUSTOMER_PATTERN, tableName);

        return new SqoopExporter.Builder() //
                .setCustomer(customer) //
                .setNumMappers(numMappers) //
                .setTable(tableName) //
                .setSourceDir(avroDir) //
                .setDbCreds(getDbCreds(sqlConfiguration)) //
                .addHadoopArg(JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT) //
                .addHadoopArg(JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .setSync(false) //
                .build();
    }

    @Override
    public SqoopExporter constructSqoopExporter(PublishTextToSqlConfiguration textToSqlConfiguration, String textDir) {
        SqlDestination destination = (SqlDestination) textToSqlConfiguration.getDestination();
        String tableName = destination.getTableName();
        String customer = String.format(EngineConstants.SQOOP_CUSTOMER_PATTERN, tableName);
        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setCustomer(customer) //
                .setNumMappers(numMappers) //
                .setTable(tableName) //
                .setSourceDir(textDir) //
                .setDbCreds(getDbCreds(textToSqlConfiguration)) //
                .addHadoopArg(JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT) //
                .addHadoopArg(JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .setSync(false) //
                .build();
        List<String> otherOptions = new ArrayList<String>();
        if (!StringUtils.isEmpty(textToSqlConfiguration.getNullString())) {
            otherOptions.add("--input-null-string");
            otherOptions.add(textToSqlConfiguration.getNullString());
            otherOptions.add("--input-null-non-string");
            otherOptions.add(textToSqlConfiguration.getNullString());
        }
        if (!StringUtils.isEmpty(textToSqlConfiguration.getEnclosedBy())) {
            otherOptions.add("--input-enclosed-by");
            otherOptions.add(textToSqlConfiguration.getEnclosedBy());
        }
        if (!StringUtils.isEmpty(textToSqlConfiguration.getOptionalEnclosedBy())) {
            otherOptions.add("--input-optionally-enclosed-by");
            otherOptions.add(textToSqlConfiguration.getOptionalEnclosedBy());
        }
        if (!StringUtils.isEmpty(textToSqlConfiguration.getEscapedBy())) {
            otherOptions.add("--input-escaped-by");
            otherOptions.add(textToSqlConfiguration.getEscapedBy());
        }
        if (!StringUtils.isEmpty(textToSqlConfiguration.getFieldTerminatedBy())) {
            otherOptions.add("--input-fields-terminated-by");
            otherOptions.add(textToSqlConfiguration.getFieldTerminatedBy());
        }
        if (!StringUtils.isEmpty(textToSqlConfiguration.getLineTerminatedBy())) {
            otherOptions.add("--input-lines-terminated-by");
            otherOptions.add(textToSqlConfiguration.getLineTerminatedBy());
        }
        exporter.setOtherOptions(otherOptions);
        return exporter;
    }

    @Override
    public String prePublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName) {
        DerivedSource source = (DerivedSource) sourceService.findBySourceName(sourceName);
        SqlDestination destination = (SqlDestination) sqlConfiguration.getDestination();
        String tableName = destination.getTableName();

        String sql = "";
        switch (sqlConfiguration.getPublicationStrategy()) {
        case VERSIONED:
        case REPLACE:
            String stageTable = tableName + STAGE_SUFFIX;
            String bakTable = tableName + BACKUP_SUFFIX;
            sql = dropTableIfExists(bakTable) + swapTableNames(tableName, bakTable) + dropTableIfExists(stageTable);
            sql += sourceColumnService.createTableSql(source, stageTable);
            break;
        case APPEND:
            break;
        }

        return sql;
    }

    @Override
    public String postPublishSql(PublishToSqlConfiguration sqlConfiguration, String sourceName) {
        DerivedSource source = (DerivedSource) sourceService.findBySourceName(sourceName);
        SqlDestination destination = (SqlDestination) sqlConfiguration.getDestination();
        String tableName = destination.getTableName();
        switch (sqlConfiguration.getPublicationStrategy()) {
        case VERSIONED:
        case REPLACE:
            String stageTable = tableName + STAGE_SUFFIX;
            String sql = sourceColumnService.createIndicesSql(source, stageTable);
            sql += swapTableNames(stageTable, tableName);
            return sql;
        case APPEND:
        default:
            return "";
        }
    }

    @Override
    public Long countPublishedTable(PublishToSqlConfiguration sqlConfiguration, JdbcTemplate jdbcTemplate) {
        SqlDestination destination = (SqlDestination) sqlConfiguration.getDestination();
        String tableName = destination.getTableName();
        return jdbcTemplate.queryForObject(countTableSql(tableName), Long.class);
    }

    @Override
    public JdbcTemplate getJdbcTemplate(PublishToSqlConfiguration sqlConfiguration) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        SQLServerMetadataProvider metadataProvider = new SQLServerMetadataProvider();
        String connection = metadataProvider.getConnectionString(getDbCreds(sqlConfiguration));
        dataSource.setUrl(connection);
        dataSource.setDriverClassName(SQLSERVER_DRIVER_CLASS);
        dataSource.setUsername(sqlConfiguration.getUsername());
        dataSource.setPassword(CipherUtils.decrypt(sqlConfiguration.getEncryptedPassword()));
        return new JdbcTemplate(dataSource);
    }

    private DbCreds getDbCreds(PublishToSqlConfiguration sqlConfiguration) {
        DbCreds.Builder builder = new DbCreds.Builder() //
                .host(sqlConfiguration.getHost()) //
                .port(sqlConfiguration.getPort()) //
                .db(sqlConfiguration.getDatabase()) //
                .user(sqlConfiguration.getUsername()) //
                .encryptedPassword(sqlConfiguration.getEncryptedPassword());
        return new DbCreds(builder);
    }

    private String dropTableIfExists(String tableName) {
        return "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + tableName
                + "') AND type in (N'U')) DROP TABLE " + tableName + ";\n";
    }

    private String swapTableNames(String srcTable, String destTable) {
        return "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'" + srcTable
                + "') AND type in (N'U')) EXEC sp_rename '" + srcTable + "', '" + destTable + "';\n";
    }

    private String countTableSql(String tableName) {
        return "SELECT COUNT(*) FROM [" + tableName + "]";
    }

}
