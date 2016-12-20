package com.latticeengines.common.exposed.util;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AMStatsUtils {
    private static final ObjectMapper om = new ObjectMapper();

    public static String compressAndEncode(Object obj) throws IOException {
        byte[] bytes = GzipUtils.compress(om.writeValueAsString(obj));
        return Base64Utils.encodeBase64(bytes);
    }

    public static <T> T decompressAndDecode(String compressed, Class<T> clazz) throws IOException {
        byte[] bytes = Base64Utils.decodeBase64(compressed);
        String json = GzipUtils.decompress(bytes);
        T obj = om.readValue(json, clazz);
        return obj;
    }
}
