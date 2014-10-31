package com.lattice.workflow.action;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.common.exposed.util.PropertyUtils;

public class LazySpringContext {

    private static ClassPathXmlApplicationContext springContext;

    private static final Log log = LogFactory.getLog(LazySpringContext.class);

    private static void initContext() {

        String dataplatformProp = "dataplatform.properties";
        String propdataProp = "propdata.properties";
        String workflowContextFile = "workflow-context.xml";

        springContext = new ClassPathXmlApplicationContext(new String[] { workflowContextFile }, false);

        Properties properties = new Properties();
        try {
            properties.load(new FileReader(dataplatformProp));
            properties.load(new FileReader(propdataProp));

        } catch (IOException ex) {
            log.error("Spring init context failed!", ex);
            return;
        }

        PropertyUtils propertyUtils = new PropertyUtils();
        propertyUtils.setProperties(properties);
        springContext.addBeanFactoryPostProcessor(propertyUtils);
        springContext.refresh();

        log.info("Spring context initialized");
    }

    public static synchronized void autowireBean(Object bean) {

        if (springContext == null) {
            synchronized (LazySpringContext.class) {
                if (springContext == null) {
                    initContext();
                }
            }
        }
        springContext.getAutowireCapableBeanFactory().autowireBean(bean);
    }
}
