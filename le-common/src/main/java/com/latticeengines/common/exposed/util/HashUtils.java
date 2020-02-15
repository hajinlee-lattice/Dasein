package com.latticeengines.common.exposed.util;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class HashUtils {

    protected HashUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getShortHash(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] thedigest = md.digest(str.getBytes(Charset.defaultCharset()));
            String digest = Base64Utils.encodeBase64(thedigest);
            return digest.substring(0, Math.min(32, digest.length()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get SHA-256 short hash of string " + str, e);
        }
    }

    public static String getMD5CheckSum(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] thedigest = md.digest(str.getBytes(Charset.defaultCharset()));
            String digest = Base64Utils.encodeBase64(thedigest);
            return digest.substring(0, Math.min(64, digest.length()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to get MD5 checksum of string " + str, e);
        }
    }

    public static String getCleanedString(String str) {
        return str.replaceAll("[^a-zA-Z0-9]+", "");
    }

    public static void main(String[] args) {
        String original = args[0];
        System.out.println(HashUtils.getCleanedString(getShortHash(original)));
    }
}
