package com.latticeengines.common.exposed.filter;

import java.io.InputStream;

import org.owasp.esapi.ESAPI;
import org.owasp.validator.html.AntiSamy;
import org.owasp.validator.html.CleanResults;
import org.owasp.validator.html.Policy;
import org.owasp.validator.html.PolicyException;
import org.owasp.validator.html.ScanException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XSSSanitizer {
    private Policy policy;
    private AntiSamy antiSamy;

    private static final Logger log = LoggerFactory.getLogger(XSSSanitizer.class);

    public XSSSanitizer(InputStream profile) throws PolicyException {
        policy = Policy.getInstance(profile);
        antiSamy = new AntiSamy();
    }

    public String Sanitize(String value) {
        try {
            // Use the ESAPI library to avoid encoded attacks.
            value = ESAPI.encoder().canonicalize(value);

            // Avoid null characterszoom
            value = value.replaceAll("\0", "");

            // Use AntiSamy for filtering
            if (antiSamy != null) {
                CleanResults cr = antiSamy.scan(value, policy);
                value = cr.getCleanHTML();
            }
        } catch(ScanException e) {
            log.warn(String.format("AntiSamey failed to scan the user input value: {%s}.", value), e);
        } catch(Exception e) {
            log.warn(String.format("Failed to sanitize the user input value: {%s}.", value), e);
        }

        return value;
    }
}
