package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.BitSet;

public class BitCodecUtils {

    public static String encode(int[] trueBits) throws IOException {
        BitSet bitSet = new BitSet();
        for (int position : trueBits) {
            bitSet.set(position, true);
        }
        return bitsToStr(bitSet);
    }

    public static boolean[] decodeAll(String bitSetStr) throws IOException {
        BitSet bitSet = strToBits(bitSetStr);
        boolean[] values = new boolean[bitSet.length()];
        for (int i = 0; i < bitSet.length(); i++) {
            values[i] = bitSet.get(i);
        }
        return values;
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
        return BitSet.valueOf(Base64Utils.decodeBase64(str));
    }

    private static String bitsToStr(BitSet bits) throws IOException {
        return Base64Utils.encodeBase64(bits.toByteArray());
    }

}
