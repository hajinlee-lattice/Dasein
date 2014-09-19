package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.Table;

public interface ImportService {
    
    void importMetadata();

    void importData();

    void init(List<Table> tables);
    
    void finalize();

}
