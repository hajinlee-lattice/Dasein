package com.latticeengines.spark.exposed.job.stats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.stats.FindChangedProfileConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class FindChangedProfileJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadData();
        FindChangedProfileConfig config = new FindChangedProfileConfig();
        SparkJobResult result = runSparkJob(FindChangedProfileJob.class, config);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> schema = Arrays.asList( //
                Pair.of(PROFILE_ATTR_ATTRNAME, String.class), //
                Pair.of(PROFILE_ATTR_BKTALGO, String.class) //
        );
        Object[][] data = new Object[][] { //
                { "attr1", "algo1" }, // not-null -> not-null (unchanged)
                { "attr2", "algo2" }, // not-null -> not-null (changed)
                { "attr3", "algo3" }, // not-null -> null
                { "attr4", null }, // null -> not-null
                { "attr5", null }, // null -> null
        };
        uploadHdfsDataUnit(data, schema);

        Object[][] data2 = new Object[][] { //
                { "attr1", "algo1" }, // not-null -> not-null (unchanged)
                { "attr2", "algo2_2" }, // not-null -> not-null (changed)
                { "attr3", null }, // not-null -> null
                { "attr4", "algo4" }, // null -> not-null
                { "attr5", null }, // null -> null
        };
        uploadHdfsDataUnit(data2, schema);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Collections.emptyList();
    }

    @Override
    protected void verifyOutput(String output) {
        List<?> lst = JsonUtils.deserialize(output, List.class);
        List<String> attrs = JsonUtils.convertList(lst, String.class);
        System.out.println(attrs);
        Set<String> keys = new HashSet<>(Arrays.asList("attr2", "attr3", "attr4"));
        Assert.assertEquals(new HashSet<>(attrs).size(), keys.size());
        for (String expected: keys) {
            Assert.assertTrue(attrs.contains(expected));
        }
    }

}
