package com.latticeengines.domain.exposed.dependency;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.metadata.DependableObject;

public interface Dependable extends HasName {
    String getType();

    List<DependableObject> getDependencies();

    void setDependencies(List<DependableObject> dependencies);
}
