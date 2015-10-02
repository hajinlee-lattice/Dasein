package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;

public interface MetadataService {

    Table getTable(CustomerSpace customerSpace, String name);
}
