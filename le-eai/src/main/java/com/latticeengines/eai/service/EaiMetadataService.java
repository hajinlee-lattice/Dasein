package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Table;

public interface EaiMetadataService {

    void registerTables(List<Table> tables, ImportContext importContext);

}
