package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy.BOOLEAN_YESNO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BooleanBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;

public class BucketTestUtils {

    private static final List<Integer> booleanLowestBits = Arrays.asList(10, 13, 16, 19);

    static List<DCEncodedAttr> EncodedAttributes() {
        // construct encoded attrs
        DCEncodedAttr attr1 = new DCEncodedAttr("EAttr1");

        DCBucketedAttr bktAttr11 = new DCBucketedAttr("IntervalInt", 0, 3);
        IntervalBucket intervalBucket = new IntervalBucket();
        intervalBucket.setBoundaries(Arrays.asList(0, 10, 100));
        bktAttr11.setBucketAlgo(intervalBucket);
        attr1.addBktAttr(bktAttr11);

        DCBucketedAttr bktAttr12 = new DCBucketedAttr("IntervalDouble", 10, 3);
        bktAttr12.setBucketAlgo(intervalBucket);
        attr1.addBktAttr(bktAttr12);

        DCEncodedAttr attr2 = new DCEncodedAttr("EAttr2");

        DCBucketedAttr bktAttr21 = new DCBucketedAttr("CatString", 3, 3);
        CategoricalBucket categoricalBucket1 = new CategoricalBucket();
        categoricalBucket1.setCategories(Arrays.asList("Value1", "Value2", "Value3"));
        bktAttr21.setBucketAlgo(categoricalBucket1);
        attr2.addBktAttr(bktAttr21);

        DCBucketedAttr bktAttr22 = new DCBucketedAttr("CatMapString", 6, 3);
        CategoricalBucket categoricalBucket2 = new CategoricalBucket();
        categoricalBucket2.setCategories(Arrays.asList("Group1", "Group2", "Group3"));
        Map<String, List<String>> mapping = new HashMap<>();
        mapping.put("Group1", Arrays.asList("Group1A", "Group1B"));
        mapping.put("Group2", Arrays.asList("Group2A", "Group2B"));
        mapping.put("Group3", Arrays.asList("Group3A", "Group3B"));
        categoricalBucket2.setMapping(mapping);
        bktAttr22.setBucketAlgo(categoricalBucket2);
        attr2.addBktAttr(bktAttr22);

        DCBucketedAttr bktAttr23 = new DCBucketedAttr("Boolean1", booleanLowestBits.get(0), 3);
        BooleanBucket booleanBucket = new BooleanBucket();
        bktAttr23.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr23);

        DCBucketedAttr bktAttr24 = new DCBucketedAttr("Boolean2", booleanLowestBits.get(1), 3);
        bktAttr24.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr24);

        DCBucketedAttr bktAttr25 = new DCBucketedAttr("Boolean3", booleanLowestBits.get(2), 3);
        bktAttr25.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr25);

        DCBucketedAttr bktAttr26 = new DCBucketedAttr("Boolean4", booleanLowestBits.get(3), 3);
        bktAttr26.setBucketAlgo(booleanBucket);
        attr2.addBktAttr(bktAttr26);

        DCEncodedAttr attr3 = new DCEncodedAttr("EAttr3");

        DCBucketedAttr bktAttr31 = new DCBucketedAttr("BitEncodeYes", 4, 2);
        bktAttr31.setBucketAlgo(booleanBucket);
        BitDecodeStrategy decodeStrategy1 = new BitDecodeStrategy();
        decodeStrategy1.setBitInterpretation(BOOLEAN_YESNO);
        decodeStrategy1.setBitPosition(3);
        decodeStrategy1.setEncodedColumn("Encoded");
        bktAttr31.setDecodedStrategy(decodeStrategy1);
        attr3.addBktAttr(bktAttr31);

        DCBucketedAttr bktAttr32 = new DCBucketedAttr("BitEncodeNo", 14, 2);
        bktAttr32.setBucketAlgo(booleanBucket);
        BitDecodeStrategy decodeStrategy2 = new BitDecodeStrategy();
        decodeStrategy2.setBitInterpretation(BOOLEAN_YESNO);
        decodeStrategy2.setBitPosition(1026);
        decodeStrategy2.setEncodedColumn("Encoded");
        bktAttr32.setDecodedStrategy(decodeStrategy2);
        attr3.addBktAttr(bktAttr32);

        return Arrays.asList(attr1, attr2, attr3);
    }

    static long setIntervalInt(long result, Integer value) {
        return setInternval(result, value, 0 ,3);
    }

    static long setIntervalDouble(long result, Double value) {
        return setInternval(result, value, 10, 3);
    }

    private static <T> long setInternval(long result, T value, int lowestBit, int numBits) {
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
        return BitCodecUtils.setBits(result, 3, 3, bucket);
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
        return BitCodecUtils.setBits(result, 6, 3, bucket);
    }

    static long setBooleans(long result, Boolean[] booleans) {
        for (int i = 0; i < booleanLowestBits.size(); i++) {
            Boolean value = booleans[i];
            result = setBooleanBit(result, booleanLowestBits.get(i), value);
        }
        return result;
    }

    private static long setBooleanBit(long result, int lowestBit, Boolean value) {
        int bucket = 0;
        if (value != null) {
            bucket = value ? 1: 2;
        }
        return BitCodecUtils.setBits(result, lowestBit, 2, bucket);
    }

    static long setYesBits(long result, int[] bits) {
        for (int b: bits) {
            switch (b) {
                case 3:
                    result = setBooleanBit(result, 4, true);
                    break;
                case 1026:
                    result = setBooleanBit(result, 14, true);
            }
        }
        return result;
    }

    static int getIntervalIntBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr1");
        return BitCodecUtils.getBits(encoded, 0, 3);
    }

    static int getIntervalDlbBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr1");
        return BitCodecUtils.getBits(encoded, 10, 3);
    }

    static int getCatStringBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 3, 3);
    }

    static int getCatMapStringBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 6, 3);
    }

    static int getBoolean1Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 10, 3);
    }

    static int getBoolean2Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 13, 3);
    }

    static int getBoolean3Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 16, 3);
    }

    static int getBoolean4Bkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr2");
        return BitCodecUtils.getBits(encoded, 19, 3);
    }

    static int getBitEncodeYesBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr3");
        return BitCodecUtils.getBits(encoded, 4, 2);
    }

    static int getBitEncodeNoBkt(GenericRecord record) {
        long encoded = (Long) record.get("EAttr3");
        return BitCodecUtils.getBits(encoded, 14, 2);
    }

}
