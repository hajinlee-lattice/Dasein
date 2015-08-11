package com.latticeengines.common.exposed.validator.impl;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.validator.AnnotationValidator;
import com.latticeengines.common.exposed.validator.annotation.RequiredKeysInMap;

public class RequiredKeysInMapAnnotationValidator implements AnnotationValidator {

    @Override
    public boolean validate(Object valueToValidate, Annotation annotation) {
        String[] requiredKeys = ((RequiredKeysInMap) annotation).keys();
        if (valueToValidate == null) {
            return false;
        }
        Collection<?> pairs = (Collection<?>) valueToValidate;
        return contains(pairs, requiredKeys);

    }

    @SuppressWarnings("unchecked")
    private boolean contains(Collection<?> pairs, String[] requiredKeys) {
        Set<String> keySet = new HashSet<String>();
        for (String key : requiredKeys) {
            keySet.add(key);
        }
        for (Object pair : pairs) {
            Map.Entry<String, ?> kv = (Map.Entry<String, ?>) pair;
            if (keySet.contains(kv.getKey()) && kv.getValue() != null && !kv.getValue().equals("")) {
                keySet.remove(kv.getKey());
            }
        }
        return keySet.size() == 0;
    }

}
