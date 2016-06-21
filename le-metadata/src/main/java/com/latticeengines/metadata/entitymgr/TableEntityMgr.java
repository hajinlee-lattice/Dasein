package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;

public interface TableEntityMgr {

    Table findByName(String name);

    void create(Table entity);

    List<Table> findAll();

    void deleteByName(String name);

    Table clone(String name);

    Table copy(String name, CustomerSpace targetCustomerSpace);
}
