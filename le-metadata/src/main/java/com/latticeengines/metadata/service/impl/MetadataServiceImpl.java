package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.MetadataService;

@Component("mdService")
public class MetadataServiceImpl implements MetadataService {
    
    @Autowired
    private TableEntityMgr tableEntityMgr;
    

    @Override
    public Table getTable(CustomerSpace customerSpace, String name) {
        return tableEntityMgr.findByName(name);
    }
    
    @Override
    public List<Table> getTables(CustomerSpace customerSpace) {
        return tableEntityMgr.findAll();
    }
}
