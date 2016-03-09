package com.latticeengines.common.exposed.util;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.binary.Base64;

public class CipherUtils {
    public static final String ENCRYPTED = "encrypted";

    // Encryption uses AES algorithm to generate 128-bit hash code
    private static final String CIPHER_METHOD = "AES";
    private static final String CIPHER_OPTS = CIPHER_METHOD + "/CBC/PKCS5Padding";

    // Secret key used for both encryption and decryption
    private static final String KEY = "I03TMIIUftFUUI7bV0zFBw==";

    private static final String CHARSET_UTF8 = "UTF-8";

    // Required by CBC block-chaining mode
    private static IvParameterSpec ivspec = new IvParameterSpec(new byte[16]);

    public static String encrypt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.ENCRYPT_MODE, strToKey(KEY), ivspec);
            return Base64.encodeBase64String(cipher.doFinal(str.getBytes(CHARSET_UTF8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String decrypt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), ivspec);
            return new String(cipher.doFinal(Base64.decodeBase64(str)), CHARSET_UTF8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function is run to set the secret key for both encryption and
     * decryption
     */
    public static String generateKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(CIPHER_METHOD);
            keyGenerator.init(128);
            return Base64.encodeBase64String(keyGenerator.generateKey().getEncoded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SecretKey strToKey(String key) {
        return new SecretKeySpec(Base64.decodeBase64(key), CIPHER_METHOD);
    }

    /**
     * This is command line tool to generate key or perform
     * encryption/decryption on strings
     */
    public static void main(String[] args) {

        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        Option encrypt = new Option("encrypt", true, " - string to encrypt");
        Option decrypt = new Option("decrypt", true, " - string to decrypt");
        Option generate = new Option("generate", false, "generate secret key");
        options.addOption(encrypt);
        options.addOption(decrypt);
        options.addOption(generate);
        try {
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("encrypt")) {
                String strToEncrypt = cmd.getOptionValue("encrypt");
                String encrypted = CipherUtils.encrypt(strToEncrypt.trim());
                System.out.println("String to Encrypt : " + strToEncrypt);
                System.out.println("Encrypted : " + encrypted);
            } else if (cmd.hasOption("decrypt")) {
                String strToDecrypt = cmd.getOptionValue("decrypt");
                String decrypted = CipherUtils.decrypt(strToDecrypt.trim());
                System.out.println("String To Decrypt : " + strToDecrypt);
                System.out.println("Decrypted : " + decrypted);
            } else if (cmd.hasOption("generate")) {
                String key = generateKey();
                System.out.println("The generated key is : " + key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}