package com.latticeengines.cassandra.exposed.dao.data;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions.Definition;

/**
 * @author LMing
 *
 */
public class ColumnTypeMapper {
    private static EnumMap<ColumnType, DataType> typeMap = new EnumMap<>(ColumnType.class);
    static {
        typeMap.put(ColumnType.ASCII, DataType.ascii());
        typeMap.put(ColumnType.BIGINT, DataType.bigint());
        typeMap.put(ColumnType.BLOB, DataType.blob());
        typeMap.put(ColumnType.BOOLEAN, DataType.cboolean());
        typeMap.put(ColumnType.COUNTER, DataType.counter());
        typeMap.put(ColumnType.DECIMAL, DataType.decimal());
        typeMap.put(ColumnType.DOUBLE, DataType.cdouble());
        typeMap.put(ColumnType.FLOAT, DataType.cfloat());
        typeMap.put(ColumnType.INET, DataType.inet());
        typeMap.put(ColumnType.INT, DataType.cint());
        typeMap.put(ColumnType.TEXT, DataType.text());
        typeMap.put(ColumnType.TIMESTAMP, DataType.timestamp());
        typeMap.put(ColumnType.TIMEUUID, DataType.timeuuid());
        typeMap.put(ColumnType.UUID, DataType.uuid());
        typeMap.put(ColumnType.VARCHAR, DataType.varchar());
        typeMap.put(ColumnType.VARINT, DataType.varint());
    }

    public static DataType toCQLType(ColumnType type) {
        DataType dataType = typeMap.get(type);
        if (dataType != null) {
            return dataType;
        }

        return DataType.text();
    }

    public static Map<String, Object> convertRowToMap(Row row) {
        if (row == null) {
            return null;
        }
        ColumnDefinitions cols = row.getColumnDefinitions();
        Map<String, Object> map = new HashMap<String, Object>(cols.size());

        for (Definition def : cols.asList()) {
            String name = def.getName();
            ByteBuffer bytesUnsafe = row.getBytesUnsafe(name);
            if (bytesUnsafe != null) {
                map.put(name, def.getType().deserialize(bytesUnsafe));
            }
        }

        return map;
    }

}
