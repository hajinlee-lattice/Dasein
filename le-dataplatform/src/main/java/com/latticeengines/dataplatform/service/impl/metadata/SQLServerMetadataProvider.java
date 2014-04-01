package com.latticeengines.dataplatform.service.impl.metadata;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.sqoop.orm.AvroSchemaGenerator;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.SQLServerManager;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

@SuppressWarnings("deprecation")
public class SQLServerMetadataProvider implements MetadataProvider {

    private AvroSchemaGenerator avroSchemaGenerator;
    
    public SQLServerMetadataProvider() {
    }

    @Override
    public String getName() {
        return "SQLServer";
    }

    @Override
    public String getConnectionString(DbCreds creds) {
        String url = "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }

        try {
            url = url.replaceFirst("\\$\\$HOST\\$\\$", creds.getHost());
            url = url.replaceFirst("\\$\\$PORT\\$\\$",
                    Integer.toString(creds.getPort()));
            url = url.replaceFirst("\\$\\$DB\\$\\$", creds.getDb());
            url = url.replaceFirst("\\$\\$USER\\$\\$", creds.getUser());
            url = url.replaceFirst("\\$\\$PASSWD\\$\\$", creds.getPassword());
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_11001, e);
        }
        return url;
    }

    @Override
    public Schema getSchema(DbCreds dbCreds, String tableName) {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(getConnectionString(dbCreds));
        ConnManager connManager = new SQLServerManager(options);
        avroSchemaGenerator = new AvroSchemaGenerator(options, connManager, tableName);
        try {
            return avroSchemaGenerator.generate();
        } catch (IOException e) {
            return null;
        }
    }

}
