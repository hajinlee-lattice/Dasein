package com.latticeengines.component.exposed.service;

import com.latticeengines.domain.exposed.component.ComponentStatus;
import com.latticeengines.domain.exposed.component.InstallDocument;

public interface ComponentService {

    boolean install(String customerSpace, InstallDocument installDocument);

    boolean update();

    boolean destroy(String customerSpace);

    void updateComponentStatus(String customerSpace, ComponentStatus status);

    ComponentStatus getComponentStatus(String customerSpace);

    boolean reset(String customerSpace);
}
