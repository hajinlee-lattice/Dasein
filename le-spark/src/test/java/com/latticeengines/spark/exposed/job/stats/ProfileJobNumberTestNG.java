package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.IntervalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.ProfileParameters;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.ProfileJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ProfileJobNumberTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        ProfileJobConfig config = prepareInput();
        SparkJobResult result = runSparkJob(ProfileJob.class, config);
        verifyResult(result);
    }

    private ProfileJobConfig prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("EvenLong", Long.class), //
                Pair.of("GeometricLong", Long.class), //
                Pair.of("DiscreteLong", Long.class), //
                Pair.of("EvenFloat", Float.class), //
                Pair.of("GeometricDouble", Double.class), //
                Pair.of("DiscreteInt", Integer.class), //
                Pair.of("EmptyInt", Long.class) //
        );

        int numRecords = 5000;
        Object[][] data = new Object[numRecords][];
        for (int i = 0; i < numRecords; i++) {
            long evenLong = i * 10 + i;
            long disLong = i % 5;
            long geoLong = (long) Math.floor(Math.pow(5, i % 11)) + i;
            float evenFloat = evenLong / 10.F;
            double geoDouble = geoLong / 100.D;
            int disInt = (int) -disLong;
            data[i] = new Object[] { evenLong, geoLong, disLong, evenFloat, geoDouble, disInt, null };
        }
        uploadHdfsDataUnit(data, fields);

        ProfileJobConfig config = new ProfileJobConfig();

        config.setNumericAttrs(getNumericAttrs(Arrays.asList( //
                "EvenLong", "GeometricLong", "DiscreteLong", //
                "EvenFloat", "GeometricDouble", "DiscreteInt", //
                "EmptyInt" //
        )));
        config.setAutoDetectDiscrete(false);
        config.setNumBucketEqualSized(false);
        config.setBucketNum(4);
        config.setMinBucketSize(2);
        config.setMaxDiscrete(5);
        config.setRandSeed(4L);
        return config;
    }

    @SuppressWarnings("SuspiciousToArrayCall")
    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            BucketAlgorithm bktAlgo = record.get(PROFILE_ATTR_BKTALGO) == null ? null
                    : JsonUtils.deserialize(record.get(PROFILE_ATTR_BKTALGO).toString(), BucketAlgorithm.class);
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
                    Assert.assertEquals(bktAlgo.getClass(), IntervalBucket.class);
                    if ("DiscreteLong".equals(attrName)) {
                        Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Integer[0]),
                                new Integer[] { 1, 2 });
                    } else {
                        Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Integer[0]),
                                new Integer[] { -3, -2, -1 });
                    }
                    break;
                case "EmptyInt":
                    Assert.assertNotNull(bktAlgo);
                    Assert.assertEquals(bktAlgo.getClass(), IntervalBucket.class);
                    Assert.assertEquals(((IntervalBucket) bktAlgo).getBoundaries().toArray(new Integer[0]),
                            new Integer[] { 0 });
                    break;
                default:
                    Assert.fail("Unknown attribute: " + record);
            }
        });
        return true;
    }

    private List<ProfileParameters.Attribute> getNumericAttrs(List<String> attrNames) {
        return attrNames.stream().map(attrName -> //
                new ProfileParameters.Attribute(attrName, null, null, new IntervalBucket()) //
        ).collect(Collectors.toList());
    }

    private List<ProfileParameters.Attribute> getCatAttrs(List<String> attrNames) {
        return attrNames.stream()
                .map(attrName -> new ProfileParameters.Attribute(attrName, null, null, new CategoricalBucket()))
                .collect(Collectors.toList());
    }

}
