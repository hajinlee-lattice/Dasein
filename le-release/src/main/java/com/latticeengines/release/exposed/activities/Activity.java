package com.latticeengines.release.exposed.activities;

import org.springframework.beans.factory.BeanNameAware;
import com.latticeengines.release.exposed.domain.StatusContext;

public interface Activity extends BeanNameAware{

    public StatusContext execute();

    public String getBeanName();
}
