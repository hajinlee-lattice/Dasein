package com.latticeengines.playmaker.entitymgr.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlaymakerRecommendationEntityMgrImplUnitTestNG {

    @Test(groups = "unit")
    public void truncateDescriptionLength() {
        // throw new RuntimeException("Test not implemented");
        List<Map<String, Object>> products = new ArrayList<Map<String, Object>>();
        Map<String, Object> myMap = new HashMap<String, Object>();
        myMap.put(PlaymakerConstants.Description,
                "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"
                        + "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"
                        + "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij");
        products.add(myMap);
        PlaymakerRecommendationEntityMgrImpl playMakerRecommendationEntityMgr = new PlaymakerRecommendationEntityMgrImpl();
        playMakerRecommendationEntityMgr.truncateDescriptionLength(products);
        products.stream().forEach(item -> {
            Assert.assertTrue(((String) item.get(PlaymakerConstants.Description)).length() <= 255);
        });
    }
}
