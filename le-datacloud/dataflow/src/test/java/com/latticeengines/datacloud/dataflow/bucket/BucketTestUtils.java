package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy.BOOLEAN_YESNO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;

public class BucketTestUtils {

    static final String ATTR_INTERVAL_INT = "IntervalInt";
    static final String ATTR_INTERVAL_DBL = "IntervalDouble";
    static final String ATTR_CAT_STR = "CatString";
    static final String ATTR_CAT_MAP_STR = "CatMapString";

    static final String ATTR_BOOLEAN_1 = "Boolean1";
    static final String ATTR_BOOLEAN_2 = "Boolean2";
    static final String ATTR_BOOLEAN_3 = "Boolean3";
    static final String ATTR_BOOLEAN_4 = "Boolean4";

    static final String ATTR_ENCODED_1 = "BitEncodeYes";
    static final String ATTR_ENCODED_2 = "BitEncodeNo";
    static final String ATTR_ENCODED_3 = "BitEncodeFake";

    static final String ATTR_DATE_1 = "Date1";
    static final long ATTR_DATE_1_CURTIME = 1540000000000L;  // 10/20/2018 01:46:40 AM GMT

    static final String ATTR_ENCODED = "Encoded";
    static final String ATTR_RELAY_STR = "RelayString";
    static final String ATTR_RELAY_INT = "RelayInteger";
    static final String ATTR_NULL_INT = "NullInteger";
    static final String ATTR_EXTRA = "Extra";
    static final String ATTR_ROW_ID = "RowID";
    static final String ATTR_RENAMED_ROW_ID = "RenamedRowId";

    private static final int BOOLEAN_NUM_BITS = 2;

    private static final Map<String, Integer> lowestBit = new HashMap<>();
    private static final Map<String, Integer> numBits = new HashMap<>();
    private static final Map<String, Integer> encodeBitPos = new HashMap<>();
    private static final Map<String, String> encAttrs = new HashMap<>();
    private static final Map<String, BucketAlgorithm> bktAlgos = new HashMap<>();

    static {
        lowestBit.put(ATTR_INTERVAL_INT, 0);
        lowestBit.put(ATTR_INTERVAL_DBL, 10);
        lowestBit.put(ATTR_CAT_STR, 3);
        lowestBit.put(ATTR_CAT_MAP_STR, 6);
        lowestBit.put(ATTR_BOOLEAN_1, 10);
        lowestBit.put(ATTR_BOOLEAN_2, 13);
        lowestBit.put(ATTR_BOOLEAN_3, 16);
        lowestBit.put(ATTR_BOOLEAN_4, 19);
        lowestBit.put(ATTR_ENCODED_1, 4);
        lowestBit.put(ATTR_ENCODED_2, 14);
        lowestBit.put(ATTR_ENCODED_3, 24);
    }

    static {
        numBits.put(ATTR_INTERVAL_INT, 3);
        numBits.put(ATTR_INTERVAL_DBL, 3);
        numBits.put(ATTR_CAT_STR, 3);
        numBits.put(ATTR_CAT_MAP_STR, 3);
        numBits.put(ATTR_BOOLEAN_1, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_BOOLEAN_2, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_BOOLEAN_3, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_BOOLEAN_4, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_ENCODED_1, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_ENCODED_2, BOOLEAN_NUM_BITS);
        numBits.put(ATTR_ENCODED_3, BOOLEAN_NUM_BITS);
    }

    static {
        encodeBitPos.put(ATTR_ENCODED_1, 3);
        encodeBitPos.put(ATTR_ENCODED_2, 1026);
        encodeBitPos.put(ATTR_ENCODED_3, 16);
    }

    static {
        encAttrs.put(ATTR_INTERVAL_INT, "EAttr1");
        encAttrs.put(ATTR_INTERVAL_DBL, "EAttr1");
        encAttrs.put(ATTR_CAT_STR, "EAttr2");
        encAttrs.put(ATTR_CAT_MAP_STR, "EAttr2");
        encAttrs.put(ATTR_BOOLEAN_1, "EAttr2");
        encAttrs.put(ATTR_BOOLEAN_2, "EAttr2");
        encAttrs.put(ATTR_BOOLEAN_3, "EAttr2");
        encAttrs.put(ATTR_BOOLEAN_4, "EAttr3");
        encAttrs.put(ATTR_ENCODED_1, "EAttr3");
        encAttrs.put(ATTR_ENCODED_2, "EAttr3");
        encAttrs.put(ATTR_ENCODED_3, "EAttr3");
    }

    static {
        bktAlgos.put(ATTR_INTERVAL_INT, intervalBucket());
        bktAlgos.put(ATTR_INTERVAL_DBL, intervalBucket());
        bktAlgos.put(ATTR_CAT_STR, categoricalBucket());
        bktAlgos.put(ATTR_CAT_MAP_STR, mapCategoricalBucket());
        bktAlgos.put(ATTR_BOOLEAN_1, new BooleanBucket());
        bktAlgos.put(ATTR_BOOLEAN_2, new BooleanBucket());
        bktAlgos.put(ATTR_BOOLEAN_3, new BooleanBucket());
        bktAlgos.put(ATTR_BOOLEAN_4, new BooleanBucket());
        bktAlgos.put(ATTR_ENCODED_1, new BooleanBucket());
        bktAlgos.put(ATTR_ENCODED_2, new BooleanBucket());
        bktAlgos.put(ATTR_ENCODED_3, new BooleanBucket());
        bktAlgos.put(ATTR_DATE_1, new DateBucket(ATTR_DATE_1_CURTIME));
    }

    static Object[][] profileData() {
        return new Object[][] { //
                relayAttr(ATTR_RENAMED_ROW_ID, ATTR_ROW_ID), //
                relayAttr(ATTR_RELAY_STR, ATTR_RELAY_STR), //
                relayAttr(ATTR_RELAY_INT, ATTR_RELAY_INT), //
                relayAttr(ATTR_NULL_INT, ATTR_NULL_INT), //
                relayAttr(ATTR_EXTRA, ATTR_EXTRA), //
                bktAttr(ATTR_INTERVAL_INT), //
                bktAttr(ATTR_INTERVAL_DBL), //
                bktAttr(ATTR_CAT_STR), //
                bktAttr(ATTR_CAT_MAP_STR), //
                bktAttr(ATTR_BOOLEAN_1), //
                bktAttr(ATTR_BOOLEAN_2), //
                bktAttr(ATTR_BOOLEAN_3), //
                bktAttr(ATTR_BOOLEAN_4), //
                bktAttr(ATTR_ENCODED_1), //
                bktAttr(ATTR_ENCODED_2), //
                bktAttr(ATTR_ENCODED_3), //
                bktAttr(ATTR_DATE_1)
        };
    }

    private static Object[] relayAttr(String attrName, String srcAttr) {
        Object[] data = new Object[7];
        data[0] = attrName;
        data[1] = srcAttr;
        if (ATTR_RELAY_INT.equals(attrName)) {
            data[6] = JsonUtils.serialize(intervalBucket());
        } else if (ATTR_NULL_INT.equals(attrName)) {
            data[6] = JsonUtils.serialize(nullDiscreteBucket());
        }
        return data;
    }

    private static Object[] bktAttr(String attrName) {
        Object[] data = new Object[7];
        data[0] = attrName;
        data[1] = attrName;
        data[2] = attrName.startsWith("BitEncode") ? JsonUtils.serialize(bitDecodeStrategy(encodeBitPos.get(attrName)))
                : null;
        data[3] = encAttrs.get(attrName);
        data[4] = lowestBit.get(attrName);
        data[5] = numBits.get(attrName);
        data[6] = JsonUtils.serialize(bktAlgos.get(attrName));
        return data;
    }

    static List<DCEncodedAttr> EncodedAttributes() {
        // construct encoded attrs
        DCEncodedAttr attr1 = new DCEncodedAttr("EAttr1");
        DCBucketedAttr bktAttr11 = newBktAttr(ATTR_INTERVAL_INT);
        attr1.addBktAttr(bktAttr11);
        DCBucketedAttr bktAttr12 = newBktAttr(ATTR_INTERVAL_DBL);
        attr1.addBktAttr(bktAttr12);

        DCEncodedAttr attr2 = new DCEncodedAttr("EAttr2");

        DCBucketedAttr bktAttr21 = newBktAttr(ATTR_CAT_STR);
        attr2.addBktAttr(bktAttr21);
        DCBucketedAttr bktAttr22 = newBktAttr(ATTR_CAT_MAP_STR);
        attr2.addBktAttr(bktAttr22);
        DCBucketedAttr bktAttr23 = newBktAttr(ATTR_BOOLEAN_1);
        attr2.addBktAttr(bktAttr23);
        DCBucketedAttr bktAttr24 = newBktAttr(ATTR_BOOLEAN_2);
        attr2.addBktAttr(bktAttr24);
        DCBucketedAttr bktAttr25 = newBktAttr(ATTR_BOOLEAN_3);
        attr2.addBktAttr(bktAttr25);
        DCBucketedAttr bktAttr26 = newBktAttr(ATTR_BOOLEAN_4);
        attr2.addBktAttr(bktAttr26);

        DCEncodedAttr attr3 = new DCEncodedAttr("EAttr3");
        DCBucketedAttr bktAttr31 = newBktAttr(ATTR_ENCODED_1);
        BitDecodeStrategy decodeStrategy1 = bitDecodeStrategy(3);
        bktAttr31.setDecodedStrategy(decodeStrategy1);
        attr3.addBktAttr(bktAttr31);
        DCBucketedAttr bktAttr32 = newBktAttr(ATTR_ENCODED_2);
        BitDecodeStrategy decodeStrategy2 = bitDecodeStrategy(1026);
        bktAttr32.setDecodedStrategy(decodeStrategy2);
        attr3.addBktAttr(bktAttr32);

        return Arrays.asList(attr1, attr2, attr3);
    }

    private static DCBucketedAttr newBktAttr(String attrName) {
        DCBucketedAttr bktAttr = new DCBucketedAttr(attrName, attrName, lowestBit.get(attrName), numBits.get(attrName));
        bktAttr.setBucketAlgo(bktAlgos.get(attrName));
        return bktAttr;
    }

    private static DiscreteBucket nullDiscreteBucket() {
        return new DiscreteBucket();
    }

    private static IntervalBucket intervalBucket() {
        IntervalBucket intervalBucket = new IntervalBucket();
        intervalBucket.setBoundaries(Arrays.asList(0, 10, 100));
        return intervalBucket;
    }

    private static CategoricalBucket categoricalBucket() {
        CategoricalBucket categoricalBucket = new CategoricalBucket();
        categoricalBucket.setCategories(Arrays.asList("Value1", "Value2", "Value3"));
        return categoricalBucket;
    }

    private static CategoricalBucket mapCategoricalBucket() {
        CategoricalBucket categoricalBucket = new CategoricalBucket();
        categoricalBucket.setCategories(Arrays.asList("Group1", "Group2", "Group3"));
        Map<String, List<String>> mapping = new HashMap<>();
        mapping.put("Group1", Arrays.asList("Group1A", "Group1B"));
        mapping.put("Group2", Arrays.asList("Group2A", "Group2B"));
        mapping.put("Group3", Arrays.asList("Group3A", "Group3B"));
        categoricalBucket.setMapping(mapping);
        return categoricalBucket;
    }

    private static BitDecodeStrategy bitDecodeStrategy(int bitPos) {
        BitDecodeStrategy decodeStrategy = new BitDecodeStrategy();
        decodeStrategy.setBitInterpretation(BOOLEAN_YESNO);
        decodeStrategy.setBitPosition(bitPos);
        decodeStrategy.setEncodedColumn(ATTR_ENCODED);
        return decodeStrategy;
    }

    static long setIntervalInt(long result, Integer value) {
        return setInterval(result, value, lowestBit.get(ATTR_INTERVAL_INT), numBits.get(ATTR_INTERVAL_INT));
    }

    static long setIntervalDouble(long result, Double value) {
        return setInterval(result, value, lowestBit.get(ATTR_INTERVAL_DBL), numBits.get(ATTR_INTERVAL_DBL));
    }

    private static <T> long setInterval(long result, T value, int lowestBit, int numBits) {
        int bucket = 0;
        if (value != null) {
            double number = Double.valueOf(value.toString());
            if (number < 0) {
                bucket = 1;
            }
            if (number >= 0) {
                bucket = 2;
            }
            if (number >= 10) {
                bucket = 3;
            }
            if (number >= 100) {
                bucket = 4;
            }
        }
        return BitCodecUtils.setBits(result, lowestBit, numBits, bucket);
    }

    static long setCatString(long result, String value) {
        int bucket = 0;
        if (value != null) {
            switch (value) {
            case "Value1":
                bucket = 1;
                break;
            case "Value2":
                bucket = 2;
                break;
            case "Value3":
                bucket = 3;
                break;
            }
        }
        return BitCodecUtils.setBits(result, lowestBit.get(ATTR_CAT_STR), numBits.get(ATTR_CAT_STR), bucket);
    }

    static long setCatMapString(long result, String value) {
        int bucket = 0;
        if (value != null) {
            switch (value.charAt(5)) {
            case '1':
                bucket = 1;
                break;
            case '2':
                bucket = 2;
                break;
            case '3':
                bucket = 3;
                break;
            }
        }
        return BitCodecUtils.setBits(result, lowestBit.get(ATTR_CAT_MAP_STR), numBits.get(ATTR_CAT_MAP_STR), bucket);
    }

    static long setBooleans(long result, Boolean[] booleans) {
        List<Integer> booleanLowestBits = Arrays.asList( //
                lowestBit.get(ATTR_BOOLEAN_1), //
                lowestBit.get(ATTR_BOOLEAN_2), //
                lowestBit.get(ATTR_BOOLEAN_3), //
                lowestBit.get(ATTR_BOOLEAN_4));
        for (int i = 0; i < booleanLowestBits.size(); i++) {
            Boolean value = booleans[i];
            result = setBooleanBit(result, booleanLowestBits.get(i), value);
        }
        return result;
    }

    private static long setBooleanBit(long result, int lowestBit, Boolean value) {
        int bucket = 0;
        if (value != null) {
            bucket = value ? 1 : 2;
        }
        return BitCodecUtils.setBits(result, lowestBit, BOOLEAN_NUM_BITS, bucket);
    }

    static long setYesBits(long result, int[] bits) {
        for (int b : bits) {
            if (b == encodeBitPos.get(ATTR_ENCODED_1)) {
                result = setBooleanBit(result, lowestBit.get(ATTR_ENCODED_1), true);
            }
            if (b == encodeBitPos.get(ATTR_ENCODED_2)) {
                result = setBooleanBit(result, lowestBit.get(ATTR_ENCODED_2), true);
            }
            if (b == encodeBitPos.get(ATTR_ENCODED_3)) {
                result = setBooleanBit(result, lowestBit.get(ATTR_ENCODED_3), true);
            }
        }
        return result;
    }

    static int getBkt(GenericRecord record, String attrName) {
        long encoded = (long) record.get(encAttrs.get(attrName));
        return BitCodecUtils.getBits(encoded, lowestBit.get(attrName), numBits.get(attrName));
    }

}
