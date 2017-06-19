package com.latticeengines.datacloud;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.swlib.SoftwarePackageInitializer;

public class Initializer implements SoftwarePackageInitializer {

    @Override
    public ApplicationContext initialize(ApplicationContext applicationContext, String module) {
        return new ClassPathXmlApplicationContext(
                new String[] { //
                        String.format("serviceflows-datacloud-%s-context.xml", getClassifier(module)), //
                        "common-am-properties-context.xml" }, //
                applicationContext);
    }

}
