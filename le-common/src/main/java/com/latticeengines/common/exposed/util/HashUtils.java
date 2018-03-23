package com.latticeengines.common.exposed.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashUtils {

    public static String getShortHash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] thedigest = md.digest(str.getBytes("UTF-8"));
            return Base64Utils.encodeBase64(thedigest).substring(0, 16);
        } catch (NoSuchAlgorithmException|UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to get SHA-256 short hash of string " + str, e);
        }
    }

}
