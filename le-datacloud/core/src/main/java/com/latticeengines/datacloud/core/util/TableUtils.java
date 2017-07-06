package com.latticeengines.datacloud.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

public class TableUtils {

    public static Table createTable(Configuration yarnConfiguration, String tableName, String avroPath) {
        Table table = MetadataConverter.getTable(yarnConfiguration, avroPath);
        table.setName(tableName);
        Extract extract = new Extract();
        String folder = StringUtils.substringAfterLast(avroPath.replace("/*.avro", ""), "/");
        extract.setName("extract_" + folder);
        extract.setPath(avroPath);
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

    public static Table createTable(Configuration yarnConfiguration, String tableName, String[] avroPaths, String[] primaryKey) {
        Table table = MetadataConverter.getTable(yarnConfiguration, avroPaths[0], null, null);
        table.setName(tableName);
        List<Extract> extractList = new ArrayList<>();
        for (String avroPath: avroPaths) {
            Extract extract = new Extract();
            String folder = StringUtils.substringAfterLast(avroPath.replace("/*.avro", ""), "/");
            extract.setName("extract_" + folder);
            extract.setPath(avroPath);
            extractList.add(extract);
        }
        table.setExtracts(extractList);
        if (primaryKey != null) {
            PrimaryKey pk = new PrimaryKey();
            pk.setAttributes(Arrays.asList(primaryKey));
            pk.setTable(table);
            table.setPrimaryKey(pk);
        }
        return table;
    }

}
