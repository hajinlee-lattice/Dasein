package com.latticeengines.dataflow.exposed.builder.operations;

import java.lang.reflect.Constructor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class GetAndValidateRealTimeTransformUtils {

    public static final String PACKATE_NAME = "com.latticeengines.transform.v2_0_25.functions";
    private static final Log log = LogFactory.getLog(GetAndValidateRealTimeTransformUtils.class);

    @SuppressWarnings("unchecked")
    public static RealTimeTransform fetchAndValidateRealTimeTransform(TransformDefinition definition) {
        RealTimeTransform transform;
        try {
            Class<RealTimeTransform> c = (Class<RealTimeTransform>) Class
                    .forName(getRTSClassFromPythonName(PACKATE_NAME, definition.name));
            Constructor<RealTimeTransform> ctor = c.getConstructor();
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
