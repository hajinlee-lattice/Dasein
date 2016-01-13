package com.latticeengines.propdata.core.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomainParser {

    static final Logger log  = LoggerFactory.getLogger(DomainParser.class);

    static public Set<String> parseDomains(String str) {
        String[] tokens = str.split(",|\\s");
        Set<String> domains = new HashSet<>();

        for (String token: tokens) {
            String domain = "";

            try {
                domain = parseOneDomain(token);
            } catch (Exception e) {
                log.warn("Failed to parse a domain from token [" + token + "]", e);
            }

            if (StringUtils.isNotEmpty(domain)) {
                domains.add(domain);
            }
        }

        return domains;
    }

    static private String parseOneDomain(String token) {
        try {
            return parseDomainUsingUri(token);
        } catch (URISyntaxException e) {
            return parseDomainUsingRegex(token);
        }
    }

    static String parseDomainUsingUri(String token) throws URISyntaxException {
        URI uri = new URI(token);
        String domain = uri.getHost();
        if (StringUtils.isNotEmpty(domain)) {
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } else {
            System.out.println(uri + " -> " + domain);
            throw new URISyntaxException(token, "The input cannot be parse to a domain.");
        }
    }

    static String parseDomainUsingRegex(String token) {
        Pattern pattern = Pattern.compile("((?!-)[A-Za-z0-9-]{1,255}(?<!-)\\.)+[A-Za-z]{2,6}");
        Matcher matcher = pattern.matcher(token);
        if (matcher.find()) {
            String domain = matcher.group(0);
            domain = StringUtils.stripStart(domain, "/");
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } else {
            return token;
        }
    }

}
