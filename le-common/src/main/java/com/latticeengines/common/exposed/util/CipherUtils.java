package com.latticeengines.common.exposed.util;

import java.util.Arrays;

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
import org.apache.commons.lang3.ArrayUtils;
import org.bitcoinj.core.Base58;

public class CipherUtils {
    public static final String ENCRYPTED = "encrypted";

    // Encryption uses AES algorithm to generate 128-bit hash code
    private static final String CIPHER_METHOD = "AES";
    private static final String CIPHER_OPTS = CIPHER_METHOD + "/CBC/PKCS5Padding";

    // Secret key used for both encryption and decryption
    private static final String KEY = "I03TMIIUftFUUI7bV0zFBw==";

    // When seen this key, means the message is salted
    private static final String SALT_HINT_STR = "bi0mpJJNxiYpEka5C6JO4g==";
    private static final byte[] SALT_HINT_BYTES = Base64.decodeBase64(SALT_HINT_STR);

    private static final String CHARSET_UTF8 = "UTF-8";

    // Required by CBC block-chaining mode
    private static IvParameterSpec ivspec = new IvParameterSpec(new byte[16]);

    public static String encrypt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            byte[] salt = generateSalt();
            cipher.init(Cipher.ENCRYPT_MODE, strToKey(KEY), new IvParameterSpec(salt));
            byte[] secret = cipher.doFinal(str.getBytes(CHARSET_UTF8));
            byte[] bytes = Arrays.copyOf(SALT_HINT_BYTES, SALT_HINT_BYTES.length);
            if ((secret.length / 16) % 2 == 1) {
                // if length is an odd number, hint + secret + salt
                bytes = ArrayUtils.addAll(bytes, secret);
                bytes = ArrayUtils.addAll(bytes, salt);
            } else {
                // if length is an even number, hint + salt + secret
                bytes = ArrayUtils.addAll(bytes, salt);
                bytes = ArrayUtils.addAll(bytes, secret);
            }
            return Base64.encodeBase64String(bytes).replace("\r", "").replace("\n", "");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String decrypt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            byte[] bytes = Base64.decodeBase64(str);
            byte[] secret;
            if (bytes.length > (SALT_HINT_BYTES.length + 16) && SALT_HINT_STR.equals(extractHint(bytes))) {
                // this is a salted secret
                byte[] salt;
                if (((bytes.length - 16 - SALT_HINT_BYTES.length) / 16) % 2 == 1) {
                    // if length is an odd number, hint + secret + salt
                    salt = Arrays.copyOfRange(bytes, bytes.length - 16, bytes.length);
                    secret = Arrays.copyOfRange(bytes, SALT_HINT_BYTES.length, bytes.length - 16);
                } else {
                    // if length is an even number, hint + salt + secret
                    salt = Arrays.copyOfRange(bytes, SALT_HINT_BYTES.length, SALT_HINT_BYTES.length + 16);
                    secret = Arrays.copyOfRange(bytes, SALT_HINT_BYTES.length + 16, bytes.length);
                }
                cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), new IvParameterSpec(salt));
                return new String(cipher.doFinal(secret), CHARSET_UTF8);
            } else {
                return decryptWithEmptySalt(str);
            }
        } catch (Exception e) {
            // fall back to old decryptor
            return decryptWithEmptySalt(str);
        }
    }

    private static String decryptWithEmptySalt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), ivspec);
            return new String(cipher.doFinal(Base64.decodeBase64(str)), CHARSET_UTF8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String extractHint(byte[] bytes) {
        return Base64.encodeBase64String(Arrays.copyOfRange(bytes, 0, SALT_HINT_BYTES.length)) //
                .replace("\r", "") //
                .replace("\n", "");
    }

    public static String encryptBase58(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.ENCRYPT_MODE, strToKey(KEY), ivspec);
            return Base58.encode(cipher.doFinal(str.getBytes(CHARSET_UTF8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String decryptBase58(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), ivspec);
            return new String(cipher.doFinal(Base58.decode(str)), CHARSET_UTF8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function is run to set the secret key for both encryption and
     * decryption
     */
    public static String generateKey() {
        return Base64.encodeBase64String(generateSalt());
    }

    private static byte[] generateSalt() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(CIPHER_METHOD);
            keyGenerator.init(128);
            return keyGenerator.generateKey().getEncoded();
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
            byte[] bytes = generateSalt();
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