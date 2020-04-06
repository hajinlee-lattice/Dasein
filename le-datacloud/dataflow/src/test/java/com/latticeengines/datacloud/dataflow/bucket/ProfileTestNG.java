package com.latticeengines.datacloud.dataflow.bucket;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.stats.Profile;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

public class ProfileTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return Profile.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() {
        ProfileParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }


    private ProfileParameters prepareInput() {
        final String encodedStr = "EncodedStr";
        final String intent1 = "Intent1";
        final String intent2 = "Intent2";

        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("EvenLong", Long.class), //
                Pair.of("GeometricLong", Long.class), //
                Pair.of("DiscreteLong", Long.class), //
                Pair.of("EvenFloat", Float.class), //
                Pair.of("GeometricDouble", Double.class), //
                Pair.of("DiscreteInt", Integer.class), //
                Pair.of("Categorical", String.class), //
                Pair.of("FreeText", String.class), //
                Pair.of(encodedStr, String.class) //
        );

        int bitUnit = 3;

        BitDecodeStrategy decode1 = new BitDecodeStrategy();
        decode1.setBitUnit(bitUnit);
        decode1.setBitPosition(3);
        decode1.setEncodedColumn(encodedStr);

        BitDecodeStrategy decode2 = new BitDecodeStrategy();
        decode2.setBitUnit(bitUnit);
        decode2.setBitPosition(6);
        decode2.setEncodedColumn(encodedStr);

        BitCodeBook bitCodeBook = new BitCodeBook();
        Map<String, Integer> bitPosMap = //
                ImmutableMap.of(intent1, decode1.getBitPosition(), intent2, decode2.getBitPosition());
        bitCodeBook.setBitsPosMap(bitPosMap);
        bitCodeBook.setDecodeStrategy(BitCodeBook.DecodeStrategy.NUMERIC_INT);
        bitCodeBook.setBitUnit(bitUnit);

        int numRecords = 5000;
        String prefix = RandomStringUtils.randomAlphanumeric(6);
        Object[][] data = new Object[numRecords][];
        for (int i = 0; i < numRecords; i++) {
            long evenLong = i * 10 + i;
            long disLong = i % 5;
            long geoLong = (long) Math.floor(Math.pow(5, i % 11)) + i;
            float evenFloat = evenLong / 10.F;
            double geoDouble = geoLong / 100.D;
            int disInt = (int) -disLong;
            String catStr = prefix + i % 100;
            String freeTextStr = RandomStringUtils.randomAlphanumeric(8);
            int ind1 = i % 3;
            int ind2 = 3 - (i % 3);
            String encStr = getEncodedString(ind1, ind2);
            Map<String, Object> decoded = bitCodeBook.decode(encStr, Arrays.asList(intent1, intent2));
            Assert.assertEquals(decoded.size(), 2);
            Assert.assertEquals(decoded.get(intent1), ind1);
            Assert.assertEquals(decoded.get(intent2), ind2);
            data[i] = new Object[] { evenLong, geoLong, disLong, evenFloat, geoDouble, disInt, catStr, freeTextStr,
                    encStr };
        }
        uploadDataToSharedAvroInput(data, fields);

        ProfileParameters parameters = new ProfileParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));

        Map<String, BitCodeBook> codeBookMap = ImmutableMap.of(encodedStr, bitCodeBook);
        Map<String, String> codeBookLookup = ImmutableMap.of(intent1, encodedStr, intent2, encodedStr);
        parameters.setCodeBookMap(codeBookMap);
        parameters.setCodeBookLookup(codeBookLookup);

        Map<String, BitDecodeStrategy> encodedAttrs = new HashMap<>();
        encodedAttrs.put("Intent1", decode1);
        encodedAttrs.put("Intent2", decode2);

        parameters.setNumericAttrs(getNumericAttrs(Arrays.asList( //
                "EvenLong", "GeometricLong", "DiscreteLong", //
                "EvenFloat", "GeometricDouble", "DiscreteInt", //
                "Intent1", "Intent2" //
        ), encodedAttrs));
        parameters.setCatAttrs(getCatAttrs(Arrays.asList("Categorical", "FreeText")));
        parameters.setNumBucketEqualSized(false);
        parameters.setBucketNum(4);
        parameters.setMinBucketSize(2);
        parameters.setMaxDiscrete(5);
        parameters.setMaxCats(100);
        parameters.setMaxCatLength(1024);
        parameters.setRandSeed(4L);
        return parameters;
    }

    private String getEncodedString(int ind1, int ind2) {
        try {
            List<Integer> trueBits = new ArrayList<>();

            trueBits.add(5); // 100
            if (ind1 == 1) { // 101
                trueBits.add(3);
            } else if (ind1 == 2) { // 110
                trueBits.add(4);
            } else if (ind1 == 3) { // 111
                trueBits.add(3);
                trueBits.add(4);
            }

            trueBits.add(8); // 100
            if (ind2 == 1) { // 101
                trueBits.add(6);
            } else if (ind2 == 2) { // 110
                trueBits.add(7);
            } else if (ind2 == 3) { // 111
                trueBits.add(6);
                trueBits.add(7);
            }

            int[] bits = new int[trueBits.size()];
            for (int i = 0; i < trueBits.size(); i++) {
                bits[i] = trueBits.get(i);
            }
            return BitCodecUtils.encode(bits);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate encoded string.", e);
        }
    }

    private List<ProfileParameters.Attribute> getNumericAttrs(List<String> attrNames, Map<String, BitDecodeStrategy> encodedAttrs) {
        return attrNames.stream().map(attrName -> {
            String decodeStrategyStr = null;
            if (encodedAttrs.containsKey(attrName)) {
                decodeStrategyStr = JsonUtils.serialize(encodedAttrs.get(attrName));
            }
            return new ProfileParameters.Attribute(attrName, null, decodeStrategyStr, new IntervalBucket());
        }).collect(Collectors.toList());
    }

    private List<ProfileParameters.Attribute> getCatAttrs(List<String> attrNames) {
        return attrNames.stream()
                .map(attrName -> new ProfileParameters.Attribute(attrName, null, null, new CategoricalBucket()))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("SuspiciousToArrayCall")
    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
//            System.out.println(record);
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            BucketAlgorithm bktAlgo = record.get(PROFILE_ATTR_BKTALGO) == null ? null
                    : JsonUtils.deserialize(record.get(PROFILE_ATTR_BKTALGO).toString(), BucketAlgorithm.class);
            Integer[] vals;
            switch (attrName) {
            case "EvenLong":
            case "EvenFloat":
                Assert.assertNotNull(bktAlgo);
                Assert.assertEquals(bktAlgo.getClass(), IntervalBucket.class);
                if ("EvenLong".equals(attrName)) {
                    Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Integer[0]),
                            new Integer[] { 15000, 30000, 45000 });
                } else {
                    Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Double[0]),
                            new Double[] { 1500., 3000., 4500. });
                }
                break;
            case "GeometricLong":
            case "GeometricDouble":
                Assert.assertNotNull(bktAlgo);
                Assert.assertEquals(bktAlgo.getClass(), IntervalBucket.class);
                if ("GeometricLong".equals(attrName)) {
                    Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Integer[0]),
                            new Integer[] { 60, 3500 });
                } else {
                    Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Double[0]),
                            new Double[] { 0.6, 35. });
                }
                break;
            case "DiscreteLong":
            case "DiscreteInt":
                Assert.assertNotNull(bktAlgo);
                Assert.assertEquals(bktAlgo.getClass(), DiscreteBucket.class);
                vals = ((DiscreteBucket) bktAlgo).getValues().toArray(new Integer[0]);
                Arrays.sort(vals);
                if ("DiscreteLong".equals(attrName)) {
                    Assert.assertEquals(vals, new Integer[]{0, 1, 2, 3, 4});
                } else {
                    Assert.assertEquals(vals, new Integer[]{-4, -3, -2, -1, 0});
                }
                break;
            case "Categorical":
                Assert.assertNotNull(bktAlgo);
                Assert.assertEquals(bktAlgo.getClass(), CategoricalBucket.class);
                Assert.assertEquals(((CategoricalBucket) bktAlgo).getCategories().size(), 100);
                break;
            case "FreeText":
                Assert.assertNull(bktAlgo);
                break;
            case "Intent1":
            case "Intent2":
                Assert.assertNotNull(bktAlgo);
                Assert.assertEquals(bktAlgo.getClass(), DiscreteBucket.class);
                vals = ((DiscreteBucket) bktAlgo).getValues().toArray(new Integer[0]);
                Arrays.sort(vals);
                if ("Intent1".equals(attrName)) {
                    Assert.assertEquals(vals, new Integer[]{0, 1, 2});
                } else {
                    Assert.assertEquals(vals, new Integer[]{1, 2, 3});
                }
                break;
            default:
                Assert.fail("Unknown attribute: " + record);
            }
        }
    }

}
