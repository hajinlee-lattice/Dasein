package com.latticeengines.common.exposed.validator;

import org.apache.commons.validator.routines.UrlValidator;

public class URLValidator extends UrlValidator {


    public boolean isValidPath(String path) {
        return super.isValidPath(path);
    }
}
