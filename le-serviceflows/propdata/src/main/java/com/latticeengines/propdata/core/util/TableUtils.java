package com.latticeengines.propdata.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtils {

    public static Table createTable(String tableName, String avroPath) {
        Table table = new Table();
        table.setName(tableName);
        Extract extract = new Extract();
        String folder = StringUtils.substringAfterLast(avroPath.replace("/*.avro", ""), "/");
        extract.setName("extract_" + folder);
        extract.setPath(avroPath);
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

    public static Table createTable(String tableName, String[] avroPaths, String[] primaryKey) {
        Table table = new Table();
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
        PrimaryKey pk = new PrimaryKey();
        pk.setAttributes(Arrays.asList(primaryKey));
        pk.setTable(table);
        table.setPrimaryKey(pk);
        return table;
    }

}
