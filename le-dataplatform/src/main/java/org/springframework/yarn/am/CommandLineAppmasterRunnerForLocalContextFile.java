package org.springframework.yarn.am;

import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class CommandLineAppmasterRunnerForLocalContextFile extends
        CommandLineAppmasterRunner {

    @Override
    protected ConfigurableApplicationContext getApplicationContext(
            String configLocation) {
        ConfigurableApplicationContext context = new FileSystemXmlApplicationContext(
                configLocation);
        context.getAutowireCapableBeanFactory().autowireBeanProperties(this,
                AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
        return context;
    }

    @Override
    protected ConfigurableApplicationContext getChildApplicationContext(
            String configLocation, ConfigurableApplicationContext parent) {
        if (configLocation != null) {
            ConfigurableApplicationContext context = new FileSystemXmlApplicationContext(
                    new String[] { configLocation }, parent);
            context.getAutowireCapableBeanFactory().autowireBeanProperties(
                    this, AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);
            return context;
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        new CommandLineAppmasterRunnerForLocalContextFile().doMain(args);
    }

}
