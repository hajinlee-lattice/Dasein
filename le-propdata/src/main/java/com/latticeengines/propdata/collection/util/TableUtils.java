package com.latticeengines.propdata.collection.util;

import java.util.Collections;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtils {

    public static Table createTable(String tableName, String avroPath) {
        Table table = new Table();
        table.setName(tableName);
        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(avroPath);
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

}
