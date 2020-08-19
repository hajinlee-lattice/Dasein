package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Category;

public class TopNAttributeTreeUnitTestNG {

    @Test(groups = "unit")
    public void testDeSer() {
        TopNAttributeTree tree = new TopNAttributeTree();
        TopNAttributes tAttrs = new TopNAttributes();
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr1", 1L));
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr2", 2L));
        tree.put(Category.FIRMOGRAPHICS, tAttrs);

        tAttrs = new TopNAttributes();
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr3", 3L));
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr4", 4L));
        tree.put(Category.GROWTH_TRENDS, tAttrs);

        tAttrs = new TopNAttributes();
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr5", 3L));
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr6", 4L));
        tree.put(Category.COVID_19, tAttrs);

        String serialized = JsonUtils.serialize(tree);

        TopNAttributeTree deserialized  = JsonUtils.deserialize(serialized, TopNAttributeTree.class);
        for (Category category : Arrays.asList(Category.FIRMOGRAPHICS, Category.GROWTH_TRENDS, Category.COVID_19)) {
            TopNAttributes topNAttributes = deserialized.get(category);
            Assert.assertEquals(topNAttributes.getTopAttributes().size(), 1);
            Assert.assertEquals(topNAttributes.getTopAttributes().get("Other").size(), 2);
        }
    }

}
