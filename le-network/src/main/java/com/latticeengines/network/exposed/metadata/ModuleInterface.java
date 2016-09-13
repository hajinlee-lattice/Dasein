package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.Module;

public interface ModuleInterface {

    Module getModule(String customerSpace, String moduleName);

}
