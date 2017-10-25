package com.latticeengines.datacloud.match.service.impl;

class MatchOutputStandardizer {
    static Object cleanNewlineCharacters(Object value) {
        Object result;

        if (value instanceof String) {
            result = ((String) value).replaceAll("(\r\n|\r|\n)+", " ");
        } else {
            result = value;
        }

        return result;
    }
}
