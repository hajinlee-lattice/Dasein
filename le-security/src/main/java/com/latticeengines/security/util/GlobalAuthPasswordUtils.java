package com.latticeengines.security.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GlobalAuthPasswordUtils {

    private static Log log = LogFactory.getLog(GlobalAuthPasswordUtils.class);

    private static final byte[] SALT = "Bard4Everyone!".getBytes();

    private GlobalAuthPasswordUtils() {

    }

    public static String EncryptPassword(String encryptThis) {
        if (encryptThis == null || encryptThis.isEmpty())
            return "";

        try {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(encryptThis.getBytes());
            byte[] hash = digest.digest(SALT);
            return DatatypeConverter.printBase64Binary(hash);

        } catch (NoSuchAlgorithmException e) {
            log.error("Can't find SHA-256 digest.");
            return "";
        }
    }

    public static String GetSecureRandomString(int length) {
        SecureRandom random = new SecureRandom();
        byte bytes[] = new byte[length];
        random.nextBytes(bytes);
        String randomStr = DatatypeConverter.printBase64Binary(bytes);
        if (randomStr.length() > length)
            randomStr = randomStr.substring(0, length);
        return randomStr;
    }

    public static String Hash256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(input.getBytes());
            byte[] hash = digest.digest();
            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < hash.length; i++) {
                hexString.append(String.format("%02x", 0xFF & hash[i]));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            log.error("Can't find SHA-256 digest.");
            return "";
        }
    }
}
