package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class PivotRatingsRuleOnlyTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private Map<String, String> idAttrMap = new HashMap<>();

    @Override
    protected String getFlowBeanName() {
        return PivotRatings.BEAN_NAME;
    }

    @Override
    protected String getScenarioName() {
        return "RuleOnly";
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
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

        TransformationFlowParameters params = new TransformationFlowParameters();
        params.setConfJson(JsonUtils.serialize(config));
        params.setBaseTables(Collections.singletonList("RuleBased"));

        return params;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            boolean hasAtLeastOneRating = false;
            for (String engine: idAttrMap.values()) {
                if (record.get(engine) != null) {
                    hasAtLeastOneRating = true;
                    break;
                }
            }
            Assert.assertTrue(hasAtLeastOneRating, //
                    String.format("%s does not have rating for any engine", JsonUtils.serialize(record)));
        }
    }

}
