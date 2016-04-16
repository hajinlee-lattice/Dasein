package com.latticeengines.propdata.engine.publication.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.dataplatform.service.impl.metadata.MetadataProvider;
import com.latticeengines.dataplatform.service.impl.metadata.SQLServerMetadataProvider;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.publication.PublishToSqlConfiguration;
import com.latticeengines.domain.exposed.propdata.publication.SqlDestination;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.engine.common.service.SourceColumnService;
import com.latticeengines.propdata.engine.publication.service.PublishConfigurationParser;
import com.latticeengines.propdata.workflow.engine.steps.EngineConstants;

@Component("publishConfigurationParser")
public class PublishConfigurationParserImpl implements PublishConfigurationParser {

    private static final String JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION = "-Dexport.statements.per.transaction=1";
    private static final String JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT = "-Dsqoop.export.records.per.statement=1000";
    private static final String SQLSERVER_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String STAGE_SUFFIX = "_Stage";
    private static final String BACKUP_SUFFIX = "_Bak";

    @Value("${propdata.collection.host}")
    private String collectionHost;

    @Value("${propdata.collection.port}")
    private int collectionPort;

    @Value("${propdata.collection.db}")
    private String collectionDb;

    @Value("${propdata.bulk.host}")
    private String bulkHost;

    @Value("${propdata.bulk.port}")
    private int bulkPort;

    @Value("${propdata.bulk.db}")
    private String bulkDb;

    @Value("${propdata.test.host}")
    private String testHost;

    @Value("${propdata.test.port}")
    private int testPort;

    @Value("${propdata.test.db}")
    private String testDb;

    @Value("${propdata.user}")
    private String dbUser;

    @Value("${propdata.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number:8}")
    private int numMappers;

    @Autowired
    private SourceService sourceService;

    @Autowired
    private SourceColumnService sourceColumnService;

    @Override
    public PublishToSqlConfiguration parseSqlAlias(PublishToSqlConfiguration sqlConfiguration) {
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
                .setSync(false) //
                .build();
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
            sql = sourceColumnService.createTableSql(source, tableName);
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
            return sourceColumnService.createIndicesSql(source, tableName);
        default:
            return "";
        }
    }

    @Override
    public JdbcTemplate getJdbcTemplate(PublishToSqlConfiguration sqlConfiguration) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        MetadataProvider metadataProvider = new SQLServerMetadataProvider();
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

}
