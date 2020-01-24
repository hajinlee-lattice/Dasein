package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.metadata.entitymgr.ModuleEntityMgr;
import com.latticeengines.metadata.service.ModuleService;

@Component("moduleService")
public class ModuleServiceImpl implements ModuleService {

    @Inject
    private ModuleEntityMgr moduleEntityMgr;

    @Override
    public Module getModuleByName(String customerSpace, String name) {
        return moduleEntityMgr.findByName(name);
    }

}
