package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelNameUtils {

    public static boolean validateModelName(String modelName) {
        Pattern pattern = Pattern.compile("[\\w-]+");
        Matcher matcher = pattern.matcher(modelName);
        return matcher.matches();
    }
}
