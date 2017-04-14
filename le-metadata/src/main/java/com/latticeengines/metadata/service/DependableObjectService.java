package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.DependableObject;

public interface DependableObjectService {
    DependableObject find(String customerSpace, String type, String name);

    void createOrUpdate(String customerSpace, DependableObject dependableObject);

    void delete(String customerSpace, String type, String name);
}
