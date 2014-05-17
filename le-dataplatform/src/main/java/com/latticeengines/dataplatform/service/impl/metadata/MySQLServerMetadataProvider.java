package com.latticeengines.dataplatform.service.impl.metadata;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.MySQLManager;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;

@SuppressWarnings("deprecation")
public class MySQLServerMetadataProvider extends MetadataProvider {

    public MySQLServerMetadataProvider() {
    }

    public String getName() {
        return "MySQL";
    }

    public String getConnectionString(DbCreds creds) {
        String url = "jdbc:mysql://$$HOST$$:$$PORT$$/$$DB$$?user=$$USER$$&password=$$PASSWD$$";
        String driverClass = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }
        return replaceUrlWithParamsAndTestConnection(url, creds);
    }

    public ConnManager getConnectionManager(SqoopOptions options) {
        return new MySQLManager(options);
    }
}
