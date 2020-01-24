package com.latticeengines.common.exposed.util;

import org.apache.commons.net.util.Base64;

public final class Base64Utils {

    protected Base64Utils() {
        throw new UnsupportedOperationException();
    }

    public static int DEFAULT_TRIM_LENGTH = 8;

    public static String encodeBase64(String input, boolean trunk, int trimLength) {
        if (input == null) {
            throw new NullPointerException("input cannot be null");
        }

        if (trimLength < 0) {
            throw new IllegalArgumentException("trim cannot be negative.");
        }

        String encodedString = Base64.encodeBase64String(input.getBytes());
        if (trunk) {
            int length = input.length() > trimLength ? trimLength : input.length();
            return encodedString.substring(0, length);
        }
        return encodedString;
    }

    public static String encodeBase64WithDefaultTrim(String input) {
        return encodeBase64(input, true, DEFAULT_TRIM_LENGTH);
    }

    public static String encodeBase64(byte[] bytes) {
        return org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(bytes);
    }

    public static byte[] decodeBase64(String base64) {
        return org.apache.commons.codec.binary.Base64.decodeBase64(base64);
    }
}
