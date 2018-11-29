package com.latticeengines.component.exposed.service;

import com.latticeengines.domain.exposed.component.ComponentStatus;

public abstract class ComponentServiceBase implements ComponentService {

    private String serviceName;

    public ComponentServiceBase(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public void updateComponentStatus(String customerSpace, ComponentStatus status) {
        //todo update service status
    }

    @Override
    public ComponentStatus getComponentStatus(String customerSpace) {
        return ComponentStatus.OK;
    }
}
