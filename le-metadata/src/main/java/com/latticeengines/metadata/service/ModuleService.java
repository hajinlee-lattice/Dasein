package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.Module;

public interface ModuleService {

    Module getModuleByName(String name);

}
