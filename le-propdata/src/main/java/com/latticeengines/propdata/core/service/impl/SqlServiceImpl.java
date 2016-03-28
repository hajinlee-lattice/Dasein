package com.latticeengines.propdata.core.service.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.ExportRequest;
import com.latticeengines.domain.exposed.propdata.ImportRequest;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.core.service.SqlService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("sqlService")
public class SqlServiceImpl implements SqlService {

    private static Log log = LogFactory.getLog(SqlServiceImpl.class);

    @Autowired
    private SqoopSyncJobService sqoopService;

    @Autowired
    private ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    private RefreshProgressEntityMgr refreshProgressEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    private JdbcTemplate jdbcTemplateBulkDB;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    private JdbcTemplate jdbcTemplateCollectionDB;

    @Value("${propdata.collection.host}")
    private String dbHost;

    @Value("${propdata.collection.port}")
    private int dbPort;

    @Value("${propdata.collection.db}")
    private String db;

    @Value("${propdata.user}")
    private String dbUser;

    @Value("${propdata.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number:8}")
    private int numMappers;

    @Override
    public ApplicationId importTable(ImportRequest importRequest, Boolean sync) {
        SqoopImporter importer = getCollectionDbImporter(
                importRequest.getSqlTable(),
                importRequest.getAvroDir(),
                "PropData",
                importRequest.getSplitColumn(),
                importRequest.getWhereClause(),
                sync);
        return sqoopService.importData(importer);
    }

    @Override
    public ApplicationId exportTable(ExportRequest exportRequest, Boolean sync) {
        SqoopExporter exporter = getCollectionDbExporter(
                exportRequest.getSqlTable(),
                exportRequest.getAvroDir(),
                "PropData",
                sync);
        return sqoopService.exportData(exporter);
    }

    private SqoopExporter getCollectionDbExporter(String sqlTable, String avroDir, String customer, Boolean sync) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);

        return new SqoopExporter.Builder()
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission())
                .setCustomer(customer + "-" + sqlTable)
                .setNumMappers(numMappers)
                .setTable(sqlTable)
                .setSourceDir(avroDir)
                .setDbCreds(new DbCreds(credsBuilder))
                .addHadoopArg("-Dsqoop.export.records.per.statement=1000")
                .addHadoopArg("-Dexport.statements.per.transaction=1")
                .addExtraOption("--batch")
                .setSync(sync)
                .build();
    }

    private SqoopImporter getCollectionDbImporter(String sqlTable, String avroDir, String customer,
                                                    String splitColumn, String whereClause, Boolean sync) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);

        SqoopImporter.Builder builder = new SqoopImporter.Builder()
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission())
                .setCustomer(customer + "-" + sqlTable)
                .setNumMappers(numMappers)
                .setSplitColumn(splitColumn)
                .setTable(sqlTable)
                .setTargetDir(avroDir)
                .setDbCreds(new DbCreds(credsBuilder))
                .setSync(sync);

        if (StringUtils.isNotEmpty(whereClause)) {
            builder = builder.addExtraOption("--where").addExtraOption(whereClause);
        }

        return builder.build();
    }

}
