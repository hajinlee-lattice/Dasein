package com.latticeengines.dataplatform.service.impl.metadata;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.SQLServerManager;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;

@SuppressWarnings("deprecation")
public class SQLServerMetadataProvider extends MetadataProvider {

    public SQLServerMetadataProvider() {
    }

    public String getName() {
        return "SQLServer";
    }

    public String getConnectionString(DbCreds creds) {
        String url = "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }
        return replaceUrlWithParamsAndTestConnection(url, creds);
    }

    public ConnManager getConnectionManager(SqoopOptions options) {
        return new SQLServerManager(options);
    }

}
