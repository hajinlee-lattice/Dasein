package com.latticeengines.metadata.hive.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

public class HiveUtils {

    public static String getCreateStatement(Configuration configuration, String tableName, Table table)
            throws IOException {
        if (table.getExtracts().size() == 0) {
            throw new RuntimeException(String.format("Table %s does not have any extracts", table.getName()));
        }
        Schema schema = AvroUtils.extractTypeInformation(MetadataConverter.getAvroSchema(configuration, table));
        String avroHdfsPath = table.getExtracts().get(0).getPath();
        String schemaHdfsPath = getParentPath(getParentPath(avroHdfsPath)) + "/schema/" + tableName + "_"
                + StringUtils.substringAfterLast(avroHdfsPath, "/").replace(".avro", ".avsc");
        if (!HdfsUtils.fileExists(configuration, schemaHdfsPath)) {
            HdfsUtils.writeToFile(configuration, schemaHdfsPath, schema.toString());
        }
        return AvroUtils.generateHiveCreateTableStatement(tableName, getLocation(table), schemaHdfsPath);
    }

    public static String getDropStatement(String name, boolean ifExists) {
        return String.format("DROP TABLE %s %s", ifExists ? "IF EXISTS" : "", name);
    }

    private static String getLocation(Table table) {
        String location = null;
        for (Extract extract : table.getExtracts()) {
            String parent = getParentPath(extract.getPath());
            if (location != null && !location.equals(parent)) {
                throw new RuntimeException(
                        String.format("Table %s cannot have extracts in different directories", table.getName()));
            }

            location = parent;
        }

        return location;
    }

    private static String getParentPath(String path) {
        return new Path(path).parent().toString();
    }
}
