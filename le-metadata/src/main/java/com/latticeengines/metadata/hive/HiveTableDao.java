package com.latticeengines.metadata.hive;

import com.latticeengines.domain.exposed.metadata.Table;

public interface HiveTableDao {
    void create(Table table);

    void deleteIfExists(Table table);

    void test(Table table);
}
