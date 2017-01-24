package com.latticeengines.metadata.hive;

import com.latticeengines.domain.exposed.metadata.Table;

public interface HiveTableDao {

    void create(Table table);

    void create(String tableName, String avroDir, String avscPath);

    void deleteIfExists(Table table);

    void deleteIfExists(String tableName);

    void test(Table table);

    void test(String tableName);
}
