package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Set;

public class EntityListCache<T> {
    private List<T> existEntities;

    private Set<String> nonExistIds;

    public List<T> getExistEntities() {
        return existEntities;
    }

    public void setExistEntities(List<T> existEntities) {
        this.existEntities = existEntities;
    }

    public Set<String> getNonExistIds() {
        return nonExistIds;
    }

    public void setNonExistIds(Set<String> nonExistIds) {
        this.nonExistIds = nonExistIds;
    }
}
