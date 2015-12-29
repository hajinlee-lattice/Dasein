package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

public class UuidUtils {

    public static String extractUuid(String modelGuid) {
        if (Strings.isNullOrEmpty(modelGuid)) {
            throw new IllegalArgumentException("The model GUID is empty");
        }
        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(modelGuid);
        if (matcher.find()) {
            return matcher.group(0);
        }
        throw new IllegalArgumentException("Cannot find uuid pattern in the model GUID " + modelGuid);
    }

    public static String parseUuid(String hdfsPath) {
        hdfsPath = PathUtils.stripoutProtocal(hdfsPath);
        String[] tokens = hdfsPath.split("/");
        return tokens[7];
    }

}
