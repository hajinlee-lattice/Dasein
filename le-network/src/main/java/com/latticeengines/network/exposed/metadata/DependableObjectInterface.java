package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.DependableObject;

public interface DependableObjectInterface {
    DependableObject find(String customerSpace, String type, String name);

    boolean createOrUpdate(String customerSpace, DependableObject dependableObject);

    boolean delete(String customerSpace, String type, String name);
}
