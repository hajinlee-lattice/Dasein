package com.latticeengines.workflow.exposed.build;

import org.springframework.beans.factory.BeanNameAware;

public class AbstractNameAwareBean implements BeanNameAware {

    private String beanName;

    @Override
    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public String name() {
        return beanName;
    }

}
