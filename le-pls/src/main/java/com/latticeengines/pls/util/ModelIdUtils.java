package com.latticeengines.pls.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ModelIdUtils {

    public static String extractUuid(String modelGuid) {
        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(modelGuid);
        if (matcher.find()) {
            return matcher.group(0);
        }
        throw new IllegalArgumentException("Cannot find uuid pattern in the model GUID " + modelGuid);
    }

}
