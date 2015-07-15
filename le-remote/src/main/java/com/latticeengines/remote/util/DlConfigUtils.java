package com.latticeengines.remote.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DlConfigUtils {
    private DlConfigUtils() { }

    public static String parseSfdcUser(String config) {
        return parseSubfieldOfDataProvider(config, "SFDC_DataProvider", "User");
    }

    public static String parseEloquaUsername(String config) {
        return parseSubfieldOfDataProvider(config, "Eloqua_DataProvider", "Username");
    }

    public static String parseEloquaCompany(String config) {
        return parseSubfieldOfDataProvider(config, "Eloqua_DataProvider", "Company");
    }

    public static String parseMarketoUserId(String config) {
        return parseSubfieldOfDataProvider(config, "Marketo_DataProvider", "UserID");
    }


    private static String parseSubfieldOfDataProvider(String config, String provider, String field) {
        Pattern pattern = Pattern.compile("<dataProvider name=\"" + provider + "\"[^>]*");
        Matcher matcher = pattern.matcher(config);
        if (matcher.find()) {
            String tag = matcher.group(0);
            pattern = Pattern.compile(field + "=[^;]*");
            matcher = pattern.matcher(tag);
            if (matcher.find()) {
                String statement = matcher.group(0);
                return statement.substring(field.length() + 1);
            } else {
                return "";
            }
        } else {
            return "";
        }
    }

}
