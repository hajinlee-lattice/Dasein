package com.latticeengines.query.util;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.StorageMechanism;
import com.latticeengines.domain.exposed.metadata.Table;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;

public final class QueryUtils {
    public static StringPath getColumnPath(Table table, Attribute attribute) {
        return getColumnPath(table, attribute.getName());
    }

    public static StringPath getColumnPath(Table table, String name) {
        StringPath tablePath = getTablePath(table);
        return Expressions.stringPath(tablePath, name);
    }

    public static StringPath getTablePath(Table table) {
        StorageMechanism storage = table.getStorageMechanism();
        String tableName = table.getName();
        if (storage instanceof JdbcStorage) {
            tableName = storage.getTableNameInStorage();
        }
        return Expressions.stringPath(tableName);
    }
}
