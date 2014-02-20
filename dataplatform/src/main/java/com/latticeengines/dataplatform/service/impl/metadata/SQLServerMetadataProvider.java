package com.latticeengines.dataplatform.service.impl.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

public class SQLServerMetadataProvider implements MetadataProvider {

    private Map<String, String> dbTypeMapping = new HashMap<String, String>();

    public SQLServerMetadataProvider() {
        dbTypeMapping.put("bigint", "int");
        dbTypeMapping.put("binary", "bytes");
        dbTypeMapping.put("bit", "int");
        dbTypeMapping.put("char", "string");
        dbTypeMapping.put("date", "string");
        dbTypeMapping.put("datetime", "string");
        dbTypeMapping.put("datetime2", "string");
        dbTypeMapping.put("datetimeoffset", "string");
        dbTypeMapping.put("decimal", "float");
        dbTypeMapping.put("float", "float");
        dbTypeMapping.put("geography", "string");
        dbTypeMapping.put("geometry", "string");
        dbTypeMapping.put("hierarchyid", "string");
        dbTypeMapping.put("image", "string");
        dbTypeMapping.put("int", "int");
        dbTypeMapping.put("money", "float");
        dbTypeMapping.put("nchar", "string");
        dbTypeMapping.put("ntext", "string");
        dbTypeMapping.put("numeric", "string");
        dbTypeMapping.put("nvarchar", "string");
        dbTypeMapping.put("real", "float");
        dbTypeMapping.put("smalldatetime", "string");
        dbTypeMapping.put("smallint", "int");
        dbTypeMapping.put("smallmoney", "float");
        dbTypeMapping.put("text", "string");
        dbTypeMapping.put("time", "string");
        dbTypeMapping.put("timestamp", "string");
        dbTypeMapping.put("tinyint", "int");
        dbTypeMapping.put("uniqueidentifier", "string");
        dbTypeMapping.put("varbinary", "bytes");
        dbTypeMapping.put("varchar", "string");
        dbTypeMapping.put("xml", "string");
    }

    @Override
    public String getName() {
        return "SQLServer";
    }

    @Override
    public Connection getConnection(DbCreds creds) {
        String url = "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
        String driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }
        Connection conn = null;

        try {
            url = url.replaceFirst("\\$\\$HOST\\$\\$", creds.getHost());
            url = url.replaceFirst("\\$\\$PORT\\$\\$",
                    Integer.toString(creds.getPort()));
            url = url.replaceFirst("\\$\\$DB\\$\\$", creds.getDb());
            url = url.replaceFirst("\\$\\$USER\\$\\$", creds.getUser());
            url = url.replaceFirst("\\$\\$PASSWD\\$\\$", creds.getPassword());
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new LedpException(LedpCode.LEDP_11001, e);
        }
        return conn;
    }

    @Override
    public String getType(String dbType) {
        return dbTypeMapping.get(dbType);
    }

    @Override
    public String getDefaultValue(String dbType) {
        String type = getType(dbType);

        if (type.equals("int")) {
            return "0";
        } else if (type.equals("string")) {
            return "null";
        } else if (type.equals("float")) {
            return "0.0";
        } else if (type.equals("bytes")) {
            return "null";
        }
        throw new LedpException(LedpCode.LEDP_11003, new String[] { dbType });
    }

}
