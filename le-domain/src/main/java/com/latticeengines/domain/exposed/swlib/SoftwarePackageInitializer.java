package com.latticeengines.domain.exposed.swlib;

import org.springframework.context.ApplicationContext;

public interface SoftwarePackageInitializer {

    ApplicationContext initialize(ApplicationContext applicationContext);
}
