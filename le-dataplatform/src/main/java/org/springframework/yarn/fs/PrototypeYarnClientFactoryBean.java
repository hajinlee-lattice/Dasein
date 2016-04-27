package org.springframework.yarn.fs;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClientFactoryBean;

/**
 * This allows for prototype instances of the yarn client. It also sets the
 * default environment and commands, assuming a Spring-based container launch.
 * 
 * @author rgonzalez
 * 
 */
public class PrototypeYarnClientFactoryBean extends YarnClientFactoryBean
        implements ApplicationContextAware {

    private ApplicationContext applicationContext;
    
    public PrototypeYarnClientFactoryBean() {
        super.setClientClass(CustomYarnClient.class);
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        CommandYarnClient systemYarnClient = (CommandYarnClient) applicationContext
                .getBean("yarnClient");
        setEnvironment(systemYarnClient.getEnvironment());
        super.afterPropertiesSet();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.applicationContext = applicationContext;
    }

}
