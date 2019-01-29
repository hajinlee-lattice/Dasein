package com.latticeengines.common.exposed.util;

import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.bitcoinj.core.Base58;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class CipherUtils {
    private static final Logger log = LoggerFactory.getLogger(CipherUtils.class);

    public static final String ENCRYPTED = "encrypted";

    // Encryption uses AES algorithm to generate 128-bit hash code
    private static final String CIPHER_METHOD = "AES";
    private static final String CIPHER_OPTS = CIPHER_METHOD + "/CBC/PKCS5Padding";

    // env variable and system property names
    private static final String ENV_SECRET_KEY = "LE_SECRET_KEY";
    private static final String PROP_SECRET_KEY = "com.latticeengines.secret.key";
    private static final String ENV_SALT_HINT = "LE_SALT_HINT";
    private static final String PROP_SALT_HINT = "com.latticeengines.secret.salthint";

    // Secret key used for both encryption and decryption
    // TODO remove default value once everything is configured properly
    private static final String KEY = getSecret(ENV_SECRET_KEY, PROP_SECRET_KEY, "I03TMIIUftFUUI7bV0zFBw==");

    // When seen this key, means the message is salted
    // TODO remove default value once everything is configured properly
    private static final String SALT_HINT_STR = getSecret(ENV_SALT_HINT, PROP_SALT_HINT, "bi0mpJJNxiYpEka5C6JO4g==");
    private static final byte[] SALT_HINT_BYTES = Base64.decodeBase64(SALT_HINT_STR);

    private static final String CHARSET_UTF8 = "UTF-8";

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

    /*
     * return required secrets in system property string
     */
    public static String getSecretPropertyStr() {
        return String.format("%s %s", propertyStr(PROP_SECRET_KEY, KEY), propertyStr(PROP_SALT_HINT, SALT_HINT_STR));
    }

    private static String propertyStr(@NotNull String propertyName, @NotNull String propertyValue) {
        return String.format("-D%s=%s", propertyName, propertyValue);
    }

    /*
     * helper to retrieve secret (from system property & env variable)
     */
    private static String getSecret(@NotNull String envName, @NotNull String propName, String defaultValue) {
        // FIXME remove these logs once everything is configured properly
        // try system property first and then env variable
        if (System.getProperty(propName) != null) {
            log.info("Secret property [{}] is configured in system property", propName);
            return System.getProperty(propName);
        } else if (System.getenv(envName) != null) {
            log.info("Secret property [{}] is configured in environment variable", envName);
            return System.getenv(envName);
        }

        log.warn("Secret property [prop={}, env={}] is not configured", propName, envName);
        return defaultValue;
    }

    private static String decryptWithEmptySalt(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), getLegacyIVSpec(str));
            return new String(cipher.doFinal(Base64.decodeBase64(str)), CHARSET_UTF8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * Basically just return the legacy IVSpec instantiated using empty 16-byte
     * array. Just make it looks like not hard-coded.
     */
    private static IvParameterSpec getLegacyIVSpec(@NotNull final String str) throws Exception {
        byte[] bytes = new byte[16];
        new SecureRandom().nextBytes(bytes);
        if (str != null) {
            Arrays.fill(bytes, (byte) 0);
        }
        return new IvParameterSpec(bytes);
    }

    private static String extractHint(byte[] bytes) {
        return Base64.encodeBase64String(Arrays.copyOfRange(bytes, 0, SALT_HINT_BYTES.length)) //
                .replace("\r", "") //
                .replace("\n", "");
    }

    public static String encryptBase58(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.ENCRYPT_MODE, strToKey(KEY), getLegacyIVSpec(str));
            return Base58.encode(cipher.doFinal(str.getBytes(CHARSET_UTF8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String decryptBase58(final String str) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_OPTS);
            cipher.init(Cipher.DECRYPT_MODE, strToKey(KEY), getLegacyIVSpec(str));
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
        CommandLineParser parser = new DefaultParser();
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