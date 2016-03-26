package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameValidationUtils {

    public static boolean validateModelName(String modelName) {
        Pattern pattern = Pattern.compile("[\\w-]+");
        Matcher matcher = pattern.matcher(modelName);
        return matcher.matches();
    }

    public static boolean validateColumnName(String columnName) {
        Pattern pattern = Pattern.compile("[\\w]+");
        Matcher matcher = pattern.matcher(columnName);
        return matcher.matches();
    }
}
