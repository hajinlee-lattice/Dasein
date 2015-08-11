package com.latticeengines.release.exposed.activities;

import org.springframework.beans.factory.BeanNameAware;

import com.latticeengines.release.exposed.domain.ProcessContext;

public interface Activity extends BeanNameAware{

    public ProcessContext execute(ProcessContext context);

    public String getBeanName();
}
