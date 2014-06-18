package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
 
public class StringTokenUtils  {

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
                if(!firsttime) {
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

}
