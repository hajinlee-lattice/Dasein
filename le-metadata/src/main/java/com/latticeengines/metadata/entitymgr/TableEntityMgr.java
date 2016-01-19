package com.latticeengines.metadata.entitymgr;

import com.latticeengines.domain.exposed.metadata.Table;

import java.util.List;

public interface TableEntityMgr {

    Table findByName(String name);

    void create(Table entity);

    List<Table> findAll();

    void deleteByName(String name);
}
