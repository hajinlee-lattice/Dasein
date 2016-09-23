package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.BitSet;

import org.xerial.snappy.Snappy;

public class BitCodecUtils {

    public static String encode(int[] trueBits) throws IOException {
        BitSet bitSet = new BitSet();
        for (int position: trueBits) {
            bitSet.set(position, true);
        }
        return bitsToStr(bitSet);
    }

    public static boolean[] decode(String bitSetStr, int[] positions) throws IOException {
        BitSet bitSet = strToBits(bitSetStr);
        boolean[] values = new boolean[positions.length];
        for (int i = 0; i < positions.length; i++) {
            int position = positions[i];
            values[i] = bitSet.get(position);
        }
        return values;
    }

    private static BitSet strToBits(String str) throws IOException {
        byte[] compressedBytes = Base64Utils.decodeBase64(str);
        byte[] uncompressedBytes = Snappy.uncompress(compressedBytes);
        return BitSet.valueOf(uncompressedBytes);
    }

    private static String bitsToStr(BitSet bits) throws IOException {
        byte[] uncompressedBytes = bits.toByteArray();
        byte[] compressedBytes = Snappy.compress(uncompressedBytes);
        return Base64Utils.encodeBase64(compressedBytes);
    }

}
