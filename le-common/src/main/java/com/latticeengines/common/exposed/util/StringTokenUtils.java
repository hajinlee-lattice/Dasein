package com.latticeengines.common.exposed.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

public final class StringTokenUtils {

    protected StringTokenUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * convert a List to String with comma-separated values
     * 
     * @param stringInList
     * @return
     */
    public static <T> String listToString(List<T> stringInList) {
        StringBuilder sb = null;
        boolean firsttime = true;
        if (stringInList != null && stringInList.size() > 0) {
            sb = new StringBuilder(stringInList.size());
            for (Iterator<T> iter = stringInList.iterator(); iter.hasNext();) {
                T feature = iter.next();
                if (!firsttime) {
                    sb.append(',');
                }
                sb.append(feature);
                firsttime = false;
            }
        }

        String str = (sb == null) ? "" : sb.toString();
        return str;
    }

    public static List<String> stringToList(String csv) {
        List<String> strList = new ArrayList<String>();
        if (csv != null) {
            StringTokenizer sTokenizer = new StringTokenizer(csv, ",");
            while (sTokenizer.hasMoreTokens()) {
                String id = sTokenizer.nextToken();
                strList.add(id);
            }
        }

        return strList;
    }

    public static String propertyToString(Properties p) {
        String serialized = "";
        if (p.isEmpty()) {
            return serialized;
        }

        for (String key : p.stringPropertyNames()) {
            serialized += key + "=" + p.getProperty(key) + " ";
        }

        return serialized.trim();
    }

    public static Properties stringToProperty(String s) {
        Properties p = new Properties();
        if (s == null) {
            return p;
        }
        String[] propertyKeyValues = s.split(" ");
        for (String propertyKeyValue : propertyKeyValues) {
            String[] kv = propertyKeyValue.split("=");
            p.setProperty(kv[0], kv[1]);
        }
        return p;
    }
    
    public static String stripPath (String path) {
        if (path.indexOf(File.separatorChar) < 0) {
            return path;
        }
        int index = path.lastIndexOf(File.separatorChar);
        // returns file name
        return path.substring(index + 1);
    }
}
