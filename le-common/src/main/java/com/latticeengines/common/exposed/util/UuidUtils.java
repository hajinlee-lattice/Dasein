package com.latticeengines.common.exposed.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Strings;

/**
 * Pack/unpack uses pipe | as delimiter.
 *
 */
public class UuidUtils {

    private static final String DELIMITER = "|";

    public static String packUuid(String... args) {
        return TokenUtils.joinAndEncrypt(DELIMITER, args);
    }

    public static Pair<String, String> unpackPairUuid(String uuid) {
        List<String> unpacked = unpackListUuid(uuid);
        return Pair.of(unpacked.get(0), unpacked.get(1));
    }

    public static List<String> unpackListUuid(String uuid) {
        return TokenUtils.decryptAndSplit(DELIMITER, uuid);
    }

    public static String extractUuid(String modelGuid) {
        if (Strings.isNullOrEmpty(modelGuid)) {
            throw new IllegalArgumentException("The model GUID is empty");
        }
        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(modelGuid);
        if (matcher.find()) {
            return matcher.group(0);
        }
        throw new IllegalArgumentException("Cannot find uuid pattern in the model GUID " + modelGuid);
    }

    public static String parseUuid(String hdfsPath) {
        hdfsPath = PathUtils.stripoutProtocol(hdfsPath);
        String[] tokens = hdfsPath.split("/");
        return tokens[7];
    }

    public static String shortenUuid(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return Base64.encodeBase64URLSafeString(bb.array()).toLowerCase();
    }
}
