package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.domain.exposed.metadata.DependableType;

public interface DependableObjectService {
    DependableObject find(String customerSpace, DependableType type, String name);

    void createOrUpdate(String customerSpace, DependableObject dependableObject);

    void delete(String customerSpace, DependableType type, String name);
}
