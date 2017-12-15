package com.latticeengines.domain.exposed.pls;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class RatingModelContainerUnitTestNG {

    @Test(groups = "unit")
    public void testSeDer() {
        RuleBasedModel model1 = new RuleBasedModel();
        AIModel model2 = new AIModel();
        String serialized = JsonUtils
                .serialize(Arrays.asList(new RatingModelContainer(model1), new RatingModelContainer(model2)));
        List<RatingModelContainer> containers = JsonUtils.convertList(JsonUtils.deserialize(serialized, List.class),
                RatingModelContainer.class);
        Assert.assertEquals(containers.get(0).getModel().getClass(), RuleBasedModel.class);
        Assert.assertEquals(containers.get(1).getModel().getClass(), AIModel.class);
    }

}
