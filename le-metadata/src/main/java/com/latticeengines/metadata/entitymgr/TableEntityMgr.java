package com.latticeengines.metadata.entitymgr;

import java.util.List;

import org.hibernate.Hibernate;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableEntityMgr {

    Table findByName(String name);

    void create(Table entity);

    List<Table> findAll();

    void deleteByName(String name);

    Table clone(String name);

    Table copy(String name, CustomerSpace targetCustomerSpace);

    Table rename(String oldName, String newName);

    static void inflateTable(Table table) {
        if (table != null) {
            Hibernate.initialize(table.getAttributes());
            Hibernate.initialize(table.getExtracts());
            Hibernate.initialize(table.getPrimaryKey());
            Hibernate.initialize(table.getLastModifiedKey());
            Hibernate.initialize(table.getHierarchies());
            Hibernate.initialize(table.getDataRules());
        }
    }

    void addExtract(Table table, Extract extract);

    void deleteTableAndCleanupByName(String name);
}
