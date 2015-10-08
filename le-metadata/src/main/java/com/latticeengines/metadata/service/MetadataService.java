package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;

public interface MetadataService {

    Table getTable(CustomerSpace customerSpace, String name);

    List<Table> getTables(CustomerSpace customerSpace);

    void createTable(CustomerSpace customerSpace, Table table);

    void deleteTable(CustomerSpace customerSpace, String name);
}
