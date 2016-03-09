package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class DomainUtils {

    private static Pattern pDomainNameOnly = Pattern.compile("((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}");

    public static String parseDomain(String url) {
        if (StringUtils.isNotEmpty(url)) {
            url = url.toLowerCase();
            while (url.contains("@")) {
                url = url.substring(url.indexOf("@") + 1);
            }
            Matcher matcher = pDomainNameOnly.matcher(url);
            if (matcher.find()) {
                String domain = matcher.group(0);
                domain = domain.startsWith("www.") ? domain.substring(4) : domain;
                if (domain.contains(".")) {
                    return domain;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

}
