package com.latticeengines.datacloud.match.service.impl;

final class MatchOutputStandardizer {

    protected MatchOutputStandardizer() {
        throw new UnsupportedOperationException();
    }
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
