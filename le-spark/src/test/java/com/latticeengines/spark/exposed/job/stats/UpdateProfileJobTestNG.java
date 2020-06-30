package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiscreteBucket;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.domain.exposed.spark.stats.UpdateProfileConfig;
import com.latticeengines.spark.exposed.utils.BucketEncodeUtils;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class UpdateProfileJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        UpdateProfileConfig config = prepareInput();
        SparkJobResult result = runSparkJob(UpdateProfileJob.class, config);
        verifyResult(result);
    }

    private UpdateProfileConfig prepareInput() {
        List<Pair<String, Class<?>>> schema = ChangeListConstants.schema();
        Object[][] data = new Object[][] { //
                // columns with changes
                { "row1", "col1", "String", null, "str1", "str2", null, null, null, null, null, null,
                        null, null, null, null },
                { "row2", "col1", "String", null, "str1", "str3", null, null, null, null, null, null,
                        null, null, null, null },
                { "row3", "col1", "String", null, null, "str4", null, null, null, null, null, null,
                        null, null, null, null },

                { "row4", "col2", "String", true, "str1", null, null, null, null, null, null, null,
                        null, null, null, null },
                { "row5", "col2", "String", true, "str2", "str3", null, null, null, null, null, null,
                        null, null, null, null },

                { "row1", "col3", "Integer", true, null, null, null, null, 1, 2, null, null,
                        null, null, null, null },
                { "row2", "col3", "Integer", true, null, null, null, null, null, 3, null, null,
                        null, null, null, null },

                { "row3", "col4", "Long", true, null, null, null, null, null, null, null, 2L,
                        null, null, null, null },
                { "row4", "col4", "Long", true, null, null, null, null, null, null, 2L, null,
                        null, null, null, null },
        };
        uploadHdfsDataUnit(data, schema);

        CategoricalBucket bkt1 = new CategoricalBucket();
        bkt1.setCategories(Arrays.asList("str100", "str101", "str102", "str103"));
        CategoricalBucket bkt2 = new CategoricalBucket();
        bkt2.setCategories(Collections.singletonList("str100"));
        DiscreteBucket bkt3 = new DiscreteBucket();
        bkt3.setValues(Arrays.asList(0, 1, 2));
        DiscreteBucket bkt4 = new DiscreteBucket();
        bkt4.setValues(Arrays.asList(10, 11, 12, 13, 14));

        List<Pair<String, Class<?>>> fields2 = BucketEncodeUtils.profileCols();
        Object[][] data2 = new Object[][] { //
                { "col1", null, null, null, null, null, JsonUtils.serialize(bkt1) },
                { "col2", null, null, null, null, null, JsonUtils.serialize(bkt2) },
                { "col3", null, null, null, null, null, JsonUtils.serialize(bkt3) },
                { "col4", null, null, null, null, null, JsonUtils.serialize(bkt4) },
        };
        uploadHdfsDataUnit(data2, fields2);

        UpdateProfileConfig config = new UpdateProfileConfig();
        config.setIncludeAttrs(Arrays.asList("col1", "col3", "col4"));
        config.setMaxDiscrete(5);
        config.setMaxCat(5);
        return config;
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            System.out.println(record);
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            BucketAlgorithm bktAlgo = record.get(PROFILE_ATTR_BKTALGO) == null ? null
                    : JsonUtils.deserialize(record.get(PROFILE_ATTR_BKTALGO).toString(), BucketAlgorithm.class);
            switch (attrName) {
                case "col1":
                    Assert.assertNull(bktAlgo);
                    break;
                case "col3":
                    Assert.assertNotNull(bktAlgo);
                    break;
                default:
                    Assert.fail("Unexpected attribute: " + record);
            }
        });
        return true;
    }

}
