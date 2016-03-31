package com.latticeengines.domain.exposed.util;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtils {
    public static Table clone(Table source) {
        Table clone = JsonUtils.clone(source);
        clone.setTableType(source.getTableType());
        return clone;
    }
}
