package com.latticeengines.common.exposed.util;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public final class TokenUtils {

    protected TokenUtils() {
        throw new UnsupportedOperationException();
    }

    public static String joinAndEncrypt(String separator, String... args) {
        Joiner joiner = Joiner.on(separator).skipNulls();
        String joined = joiner.join(args);
        String encrypted = CipherUtils.encryptBase58(joined);

        return encrypted;
    }

    public static List<String> decryptAndSplit(String separator, String encryptedStringToSplit) {
        String decrypted = CipherUtils.decryptBase58(encryptedStringToSplit);
        Iterable<String> iter = Splitter.on(separator).trimResults().omitEmptyStrings().split(decrypted);

        return Lists.newArrayList(iter);
    }
}
