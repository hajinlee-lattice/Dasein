package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PivotRatingsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PivotRatingsTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "pivotRatings";
    }

    @Override
    protected String getScenarioName() {
        return "cdl";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("AIBased", "RuleBased", "PivotedRating");
    }

    @Test(groups = "functional")
    public void runTest() {
        PivotRatingsConfig config = new PivotRatingsConfig();
        config.setIdAttrsMap(ImmutableMap.of( //
                "rule_y2dvrdmfqc2f6tsl1uqc7g", "engine_rule1", //
                "rule_yt0b97azssea_i_gch6cww", "engine_rule2", //
                "ai_dy4cicoutpw_bmraclvlfg", "engine_ai3", //
                "ai_lhydyrfaq52ro_pke_t8aa", "engine_ai4"));
        config.setInactiveEngineIds(Arrays.asList("engine_xnqw2vq_r_mnpfmmf77xfg", "engine_rg8xwifnrci8_mffadzwaw"));
        config.setEvModelIds(Collections.singletonList("ai_dy4cicoutpw_bmraclvlfg"));
        config.setAiModelIds(Arrays.asList("ai_dy4cicoutpw_bmraclvlfg", "ai_lhydyrfaq52ro_pke_t8aa"));
        config.setAiSourceIdx(0);
        config.setRuleSourceIdx(1);
        config.setInactiveSourceIdx(2);
        SparkJobResult result = runSparkJob(PivotRatings.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            List<String> attrs = record.getSchema().getFields().stream() //
                    .map(Schema.Field::name).collect(Collectors.toList());
            Assert.assertFalse(attrs.contains("ai_score__AccountId"));
            Assert.assertFalse(attrs.contains("ai_ev__AccountId"));
            Assert.assertFalse(attrs.contains("ai_rating__ai_score__ai_ev__AccountId"));

            Assert.assertTrue(attrs.contains("engine_rule1"));
            Assert.assertFalse(attrs.contains("engine_rule1_ev"));
            Assert.assertFalse(attrs.contains("engine_rule1_pv"));
            Assert.assertFalse(attrs.contains("engine_rule1_score"));

            Assert.assertTrue(attrs.contains("engine_rule2"));
            Assert.assertFalse(attrs.contains("engine_rule2_ev"));
            Assert.assertFalse(attrs.contains("engine_rule2_pv"));
            Assert.assertFalse(attrs.contains("engine_rule2_score"));

            Assert.assertTrue(attrs.contains("engine_ai3"));
            Assert.assertTrue(attrs.contains("engine_ai3_ev"));
            Assert.assertTrue(attrs.contains("engine_ai3_pv"));
            Assert.assertTrue(attrs.contains("engine_ai3_score"));

            Assert.assertTrue(attrs.contains("engine_ai4"));
            Assert.assertFalse(attrs.contains("engine_ai4_ev"));
            Assert.assertFalse(attrs.contains("engine_ai4_pv"));
            Assert.assertTrue(attrs.contains("engine_ai4_score"));

            Assert.assertTrue(attrs.contains("engine_xnqw2vq_r_mnpfmmf77xfg"));
            Assert.assertTrue(attrs.contains("engine_rg8xwifnrci8_mffadzwaw"));
            Assert.assertTrue(attrs.contains("engine_rg8xwifnrci8_mffadzwaw_score"));
            Assert.assertTrue(attrs.contains("engine_rg8xwifnrci8_mffadzwaw_ev"));
            Assert.assertTrue(attrs.contains("engine_rg8xwifnrci8_mffadzwaw_pv"));
        });
        Assert.assertEquals(count.get(), 612);
        return true;
    }

}
