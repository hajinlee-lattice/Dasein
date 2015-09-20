package com.latticeengines.prospectdiscovery.dataflow;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;

public class Initializer implements SoftwarePackageInitializer {

    @SuppressWarnings("resource")
    @Override
    public void initialize() {
        new ClassPathXmlApplicationContext("serviceflows-prospectdiscovery-context.xml");
    }

}
