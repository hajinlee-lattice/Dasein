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
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr1", 1));
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr2", 2));
        tree.put(Category.FIRMOGRAPHICS, tAttrs);

        tAttrs = new TopNAttributes();
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr3", 3));
        tAttrs.addTopAttribute("Other", new TopNAttributes.TopAttribute("attr4", 4));
        tree.put(Category.GROWTH_TRENDS, tAttrs);

        String serialized = JsonUtils.serialize(tree);

        TopNAttributeTree deserialized  = JsonUtils.deserialize(serialized, TopNAttributeTree.class);
        for (Category category: Arrays.asList(Category.FIRMOGRAPHICS, Category.GROWTH_TRENDS)) {
            TopNAttributes topNAttributes = deserialized.get(category);
            Assert.assertEquals(topNAttributes.getTopAttributes().size(), 1);
            Assert.assertEquals(topNAttributes.getTopAttributes().get("Other").size(), 2);
        }
    }

}
