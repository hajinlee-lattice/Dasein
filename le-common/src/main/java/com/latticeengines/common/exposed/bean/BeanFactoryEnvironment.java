package com.latticeengines.common.exposed.bean;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.TestClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BeanFactoryEnvironment {

    private static Logger log = LoggerFactory.getLogger(BeanFactoryEnvironment.class);

    private static Environment environment = null;

    public static void setEnvironment(String environmentName) {
        Environment environment1 = Environment.valueOf(environmentName);
        if (environment == null || TestClient.equals(environment)) {
            environment = environment1;
            log.info("Set BeanFactoryEnvironment to " + environment);
        } else if (!environment.equals(environment1)) {
            String error = "BeanFactoryEnvironment is already set to " + environment + ", cannot switch to " + environment1;
            if (TestClient.equals(environment1)) {
                log.warn(error);
            } else {
                throw new IllegalArgumentException(error);
            }
        }
    }

    public static Environment getEnvironment() {
        return environment;
    }

    public enum Environment {
        WebApp, AppMaster, TestClient
    }

}
