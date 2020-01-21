package com.latticeengines.common.exposed.bean;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.TestClient;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BeanFactoryEnvironment {

    private static final Logger log = LoggerFactory.getLogger(BeanFactoryEnvironment.class);

    private static Environment environment;

    private static String service;

    public static void setEnvironment(String environmentName) {
        Environment environment1 = Environment.valueOf(environmentName);
        if (environment == null || TestClient.equals(environment)) {
            environment = environment1;
            log.info("Set BeanFactoryEnvironment to " + environment);
        } else if (!environment.equals(environment1)) {
            String error = "BeanFactoryEnvironment is already set to " + environment //
                    + ", cannot switch to " + environment1;
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

    public static void setService(String service) {
        if (StringUtils.isBlank(BeanFactoryEnvironment.service)) {
            BeanFactoryEnvironment.service = service;
            log.info("Set service in the bean environment to be " + getService());
        }
    }

    public static String getService() {
        return service;
    }

    public enum Environment {
        WebApp, AppMaster, TestClient
    }

}
