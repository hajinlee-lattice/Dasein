package com.latticeengines.apps.cdl.util;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public final class ValidateFileHeaderUtils {

    protected ValidateFileHeaderUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String CSV_INJECTION_CHARACHTERS = "@+-=";

    public static void checkForCSVInjectionInFileNameAndHeaders(String fileDisplayName, Set<String> headers) {
        if (CSV_INJECTION_CHARACHTERS.indexOf(fileDisplayName.charAt(0)) != -1) {
            throw new LedpException(LedpCode.LEDP_18208);
        }
        for (String header : headers) {
            if (StringUtils.isNotBlank(header) && CSV_INJECTION_CHARACHTERS.indexOf(header.charAt(0)) != -1) {
                throw new LedpException(LedpCode.LEDP_18208);
            }
        }
    }
}
