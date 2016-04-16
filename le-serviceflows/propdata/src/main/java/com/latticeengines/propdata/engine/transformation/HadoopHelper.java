package com.latticeengines.propdata.engine.transformation;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.propdata.core.service.SqoopService;
import com.latticeengines.propdata.core.util.LoggingUtils;

@Component
public class HadoopHelper extends HdfsHelper {
    private static final int SECONDS_IN_24_HOURS = 24 * 3600;

    private static final String SQOOP_OPTION_WHERE = "--where";

    private static final String SQOOP_OPTION_BATCH = "--batch";

    private static final String JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION = "-Dexport.statements.per.transaction=1";

    private static final String JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT = "-Dsqoop.export.records.per.statement=1000";

    private static final String SQOOP_CUSTOMER_PROPDATA = "PropData-";

    @Autowired
    protected SqoopService sqoopService;

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

    public boolean importFromCollectionDB(String table, String targetDir, String splitColumn, String whereClause,
            Progress progress, Log logger) {
        try {
            SqoopImporter importer = getCollectionDbImporter(table, targetDir, splitColumn, whereClause);
            ApplicationId appId = sqoopService.importTable(importer);
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(getYarnConfiguration(), appId,
                    SECONDS_IN_24_HOURS);
            if (!FinalApplicationStatus.SUCCEEDED.equals(status)) {
                throw new IllegalStateException("The final state of " + appId + " is not "
                        + FinalApplicationStatus.SUCCEEDED + " but rather " + status);
            }

        } catch (Exception e) {
            LoggingUtils.logError(logger, progress, "Failed to import data from source DB.", e);
            return false;
        }
        return true;
    }

    public SqoopExporter getCollectionDbExporter(String sqlTable, String avroDir) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));

        return new SqoopExporter.Builder().setCustomer(SQOOP_CUSTOMER_PROPDATA + sqlTable).setNumMappers(numMappers)
                .setTable(sqlTable).setSourceDir(avroDir).setDbCreds(new DbCreds(credsBuilder))
                .addHadoopArg(JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT)
                .addHadoopArg(JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION).addExtraOption(SQOOP_OPTION_BATCH)
                .setSync(false).build();
    }

    public SqoopImporter getCollectionDbImporter(String sqlTable, String avroDir, String splitColumn,
            String whereClause) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));

        SqoopImporter.Builder builder = new SqoopImporter.Builder().setCustomer(SQOOP_CUSTOMER_PROPDATA + sqlTable)
                .setNumMappers(numMappers).setSplitColumn(splitColumn).setTable(sqlTable).setTargetDir(avroDir)
                .setDbCreds(new DbCreds(credsBuilder)).setSync(false);

        if (StringUtils.isNotEmpty(whereClause)) {
            builder = builder.addExtraOption(SQOOP_OPTION_WHERE).addExtraOption(whereClause);
        }

        return builder.build();
    }

    public ApplicationId exportTable(SqoopExporter exporter) {
        return sqoopService.exportTable(exporter);
    }
}
