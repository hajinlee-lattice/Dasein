package com.latticeengines.dataflow.exposed.builder.strategy;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public interface KVAttrPicker extends Serializable {
    Collection<String> helpFieldNames();

    String valClzSimpleName();

    Object updateHelpAndReturnValue(Object oldValue, Map<String, Object> oldHelp, Object newValue,
            Map<String, Object> newHelp);
}
