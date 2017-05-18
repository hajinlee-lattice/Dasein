package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.BitSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

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

    public static long setBits(long result, int lowestBitPos, int numBits, int value) {
        for (int i = 0; i < numBits; i++) {
            int offset = lowestBitPos + i;
            if ((value >> i & 1L) == 1) {
                result = result | (1L << offset);
            } else {
                result = result & ~(1L << offset);
            }
        }
        return result;
    }

    public static long bitMask(long result, int lowestBitPos, int numBits) {
        for (int i = 0; i < numBits; i++) {
            int offset = lowestBitPos + i;
            result = result | (1L << offset);
        }
        return result;
    }

    public static int getBits(long result, int lowestBitPos, int numBits) {
        int value = 0;
        for (int i = 0; i < numBits; i++) {
            int offset = lowestBitPos + i;
            if ((result >> offset & 1L) == 1) {
                value = value | (1 << i);
            }
        }
        return value;
    }

    /**
     * This is command line tool to decode an encoded base64 string
     */
    public static void main(String[] args) throws ParseException {
        Option posOption = new Option("b", true, " bit position");
        Option strOption = new Option("s", true, " encoded string");

        Options options = new Options();
        options.addOption(posOption);
        options.addOption(strOption);

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(options, args);

        Integer bitPos = Integer.valueOf(cmdLine.getOptionValue("b"));
        String str = cmdLine.getOptionValue("s");

        System.out.println(String.format("Decoding %d-th bit in [%s]", bitPos, str));
    }
}
