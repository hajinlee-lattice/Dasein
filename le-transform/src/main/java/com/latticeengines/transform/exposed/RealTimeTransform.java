package com.latticeengines.transform.exposed;

import java.io.Serializable;
import java.util.Map;
import com.latticeengines.domain.exposed.metadata.Attribute;

public interface RealTimeTransform extends Serializable{

    Object transform(Map<String, Object> arguments, Map<String, Object> record);

    Attribute getMetadata();
}
