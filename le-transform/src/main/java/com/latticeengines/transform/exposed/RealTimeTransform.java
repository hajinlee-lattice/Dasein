package com.latticeengines.transform.exposed;

import java.io.Serializable;
import java.util.Map;

import com.latticeengines.transform.exposed.metadata.TransformMetadata;

public interface RealTimeTransform extends Serializable{

    Object transform(Map<String, Object> arguments, Map<String, Object> record);

    TransformMetadata getMetadata();
}
