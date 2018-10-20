package com.latticeengines.domain.exposed.dataplatform;

import java.util.Map.Entry;
import java.util.Set;

public interface HasProperty {

    Object getPropertyValue(String key);

    void setPropertyValue(String key, Object value);

    Set<Entry<String, Object>> getEntries();
}
