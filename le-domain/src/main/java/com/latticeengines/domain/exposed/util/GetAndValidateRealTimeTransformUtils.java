package com.latticeengines.domain.exposed.util;

import java.lang.reflect.Constructor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;

public class GetAndValidateRealTimeTransformUtils {

    private static final Log log = LogFactory.getLog(GetAndValidateRealTimeTransformUtils.class);

    @SuppressWarnings("unchecked")
    public static <T> T fetchAndValidateRealTimeTransform(TransformDefinition definition, String packageName) {
        T transform;
        try {
            Class<T> c = (Class<T>) Class.forName(getRTSClassFromPythonName(packageName, definition.name));
            Constructor<T> ctor = c.getConstructor();
            transform = ctor.newInstance();
        } catch (Exception e1) {
            log.error(e1);
            throw new RuntimeException(e1);
        }
        return transform;
    }

    private static String getRTSClassFromPythonName(String packageName, String pythonModuleName) {
        String[] tokens = pythonModuleName.split("_");
        StringBuilder sb = new StringBuilder(packageName + ".");

        for (String token : tokens) {
            sb.append(StringUtils.capitalize(token));
        }
        return sb.toString();
    }

}
