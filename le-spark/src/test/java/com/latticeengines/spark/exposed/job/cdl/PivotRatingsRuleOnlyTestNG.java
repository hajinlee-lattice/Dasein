package com.latticeengines.spark.exposed.job.cdl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PivotRatingsRuleOnlyTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "pivotRatings";
    }

    @Override
    protected String getScenarioName() {
        return "RuleOnly";
    }

    private Map<String, String> idAttrMap = new HashMap<>();

    @Test(groups = "functional")
    public void runTest() {
        PivotRatingsConfig config = new PivotRatingsConfig();
        idAttrMap.put("rule_u5sprbzit7enikkc2iaykg", "engine_1");
        idAttrMap.put("rule_dgpranosrkez5y_jptb81g", "engine_2");
        idAttrMap.put("rule_zlklelckqmgjmfrdywyh4q", "engine_3");
        idAttrMap.put("rule_3vkx5rrmqrsgcnjyxibvmg", "engine_4");
        idAttrMap.put("rule_nw02vux9tboi2dmfi2wsfw", "engine_5");
        idAttrMap.put("rule_djthyqhjq3akowoqgh3oyg", "engine_6");
        config.setIdAttrsMap(idAttrMap);
        config.setEvModelIds(Collections.emptyList());
        config.setAiModelIds(Collections.emptyList());
        config.setRuleSourceIdx(0);
        SparkJobResult result = runSparkJob(PivotRatings.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            boolean hasAtLeastOneRating = false;
            for (String engine: idAttrMap.values()) {
                if (record.get(engine) != null) {
                    hasAtLeastOneRating = true;
                    break;
                }
            }
            Assert.assertTrue(hasAtLeastOneRating, //
                    String.format("%s does not have rating for any engine", record));
        });
        Assert.assertEquals(count.get(), 3179);
        return true;
    }

}
