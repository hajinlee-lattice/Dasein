package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;

public class PlaymakerRecommendationEntityMgrImplUnitTestNG {

    @Test(groups = "unit")
    public void truncateDescriptionLength() {
        // throw new RuntimeException("Test not implemented");
        List<Map<String, Object>> products = new ArrayList<Map<String, Object>>();
        Map<String, Object> myMap = new HashMap<String, Object>();
        myMap.put(PlaymakerConstants.Description,
                "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"
                        + "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"
                        + "123456789A123456789A123456789A123456789A123456789A123456789A123456789A123456789A123456789A123456789A");
        products.add(myMap);
        PlaymakerRecommendationEntityMgrImpl playMakerRecommendationEntityMgr = new PlaymakerRecommendationEntityMgrImpl();
        playMakerRecommendationEntityMgr.truncateDescriptionLength(products);
        products.forEach(item -> {
            Assert.assertTrue(((String) item.get(PlaymakerConstants.Description)).length() <= 255);
            System.out.println(item.get(PlaymakerConstants.Description));
        });
    }
}
