package com.latticeengines.common.exposed.filter;

import org.owasp.esapi.ESAPI;
import org.owasp.validator.html.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XSSSanitizer {
    private Policy policy;
    private AntiSamy antiSamy;

    private static final Logger log = LoggerFactory.getLogger(XSSFilter.XssRequestWrapper.class);

    public XSSSanitizer(String antiSamyProfile) {
        try {
            policy = Policy.getInstance(antiSamyProfile);
            antiSamy = new AntiSamy();
        } catch (PolicyException e) {
            log.warn(String.format("Failed to initialize AntiSamy Policy from file: {%s}.", antiSamyProfile), e);
        }
    }

    public String Sanitize(String value) {
        try {
            // Use the ESAPI library to avoid encoded attacks.
            value = ESAPI.encoder().canonicalize(value);

            // Avoid null characterszoom
            value = value.replaceAll("\0", "");

            // Use AntiSamy for filtering
            CleanResults cr = antiSamy.scan(value, policy);
            value = cr.getCleanHTML();
        } catch(ScanException e) {
            log.warn(String.format("AntiSamey failed to scan the user input value: {%s}.", value), e);
        } catch(Exception e) {
            log.warn(String.format("Failed to sanitize the user input value: {%s}.", value), e);
        }

        return value;
    }
}
