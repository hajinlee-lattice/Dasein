package com.latticeengines.prospectdiscovery;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;

public class Initializer implements SoftwarePackageInitializer {

    @Override
    public ApplicationContext initialize(ApplicationContext applicationContext) {
        return new ClassPathXmlApplicationContext(new String[] { "serviceflows-prospectdiscovery-context.xml" }, // 
                applicationContext);
    }

}
