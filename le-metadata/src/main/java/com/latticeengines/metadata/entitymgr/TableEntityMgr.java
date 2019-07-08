package com.latticeengines.metadata.entitymgr;

import java.util.List;

import org.hibernate.Hibernate;
import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.AttributeFixer;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableEntityMgr {

    Table findByName(String name);

    Table findByName(String name, boolean inflate);

    Table findByName(String name, boolean inflate, boolean includeAttributes);

    void create(Table entity);

    void addAttributes(String name, List<Attribute> attributes);

    List<Table> findAll();

    void deleteByName(String name);

    Table clone(String name, boolean ignoreExtracts);

    Table copy(String name, CustomerSpace targetCustomerSpace);

    Table rename(String oldName, String newName);

    static void inflateTable(Table table) {
        inflateTable(table, true);
    }

    static void inflateTable(Table table, boolean includeAttributes) {
        if (table != null) {
            if (includeAttributes) {
                Hibernate.initialize(table.getAttributes());
            }
            Hibernate.initialize(table.getExtracts());
            Hibernate.initialize(table.getPrimaryKey());
            Hibernate.initialize(table.getLastModifiedKey());
            Hibernate.initialize(table.getHierarchies());
            Hibernate.initialize(table.getDataRules());
        }
    }

    void addExtract(Table table, Extract extract);

    void deleteTableAndCleanupByName(String name);

    Long countAttributesByTable_Pid(Long tablePid);

    List<Attribute> findAttributesByTable_Pid(Long tablePid, Pageable pageable);

    void fixAttributes(String name, List<AttributeFixer> attributeFixerList);

}
