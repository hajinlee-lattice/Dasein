package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.Table;

public interface ImportService {
    
    List<Table> importMetadata(List<Table> tables);

    void importData(List<Table> tables);

}
