package com.latticeengines.common.exposed.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.EmailValidator;

public class DomainUtils {

    private static Pattern pDomainNameOnly = Pattern.compile("((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,}");
    private static EmailValidator emailValidator = EmailValidator.getInstance();

    public static String parseDomain(String url) {
        if (StringUtils.isEmpty(url)
                || url.trim().equalsIgnoreCase("none")
                || url.trim().equalsIgnoreCase("null")) {
            return null;
        }

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

        return null;
    }

    public static boolean isEmail(String url) {
        if (url != null && url.contains("@")) {
            return true;
        } else {
            return false;
        }
    }

    public static String parseEmail(String email) {
        if (StringUtils.isBlank(email)) {
            return null;
        }
        email = email.replaceAll("\\s", "");
        if (emailValidator.isValid(email)) {
            return email;
        } else {
            return null;
        }
    }
}
