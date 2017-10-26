package com.latticeengines.common.exposed.bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BeanFactoryEnvironment {

    private static Logger log = LoggerFactory.getLogger(BeanFactoryEnvironment.class);

    private static Environment environment;

    public static void setEnvironment(String environmentName) {
        Environment environment1 = Environment.valueOf(environmentName);
        if (environment == null) {
            environment = environment1;
            log.info("Set BeanFactoryEnvironment to " + environment);
        } else if (!environment.equals(environment1)) {
            throw new IllegalArgumentException("BeanFactoryEnvironment is already set to " + environment + ", cannot switch to " + environment1);
        }
    }

    public static Environment getEnvironment() {
        return environment;
    }

    public enum Environment {
        WebApp, AppMaster, TestClient
    }

}
