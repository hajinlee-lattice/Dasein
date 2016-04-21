package com.latticeengines.transform.exposed;

import java.util.Map;

public interface RealTimeTransform {

    Object transform(Map<String, Object> arguments, Map<String, Object> record);
}
