package com.latticeengines.metadata.hive.util;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

public class HiveUtils {
    public static String getCreateStatement(Configuration configuration, String tableName, Table table) {
        return AvroUtils.generateHiveCreateTableStatement(tableName, getLocation(table),
                MetadataConverter.getAvroSchema(configuration, table));
    }

    public static String getDropStatement(String name, boolean ifExists) {
        return String.format("DROP TABLE %s %s", ifExists ? "IF EXISTS" : "", name);
    }

    private static String getLocation(Table table) {
        String location = null;
        for (Extract extract : table.getExtracts()) {
            String parent = getParentPath(extract.getPath());
            if (location != null && !location.equals(parent)) {
                throw new RuntimeException(String.format("Table %s cannot have extracts in different directories",
                        table.getName()));
            }

            location = parent;
        }

        return location;
    }

    private static String getParentPath(String path) {
        return new Path(path).parent().toString();
    }
}
