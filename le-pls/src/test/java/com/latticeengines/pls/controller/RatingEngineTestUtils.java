package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;

public class RatingEngineTestUtils {

    public static List<RatingEngine> createRatingEngineList() {
        List<RatingEngine> list = new ArrayList<>();
        RatingEngine re1 = new RatingEngine();
        list.add(re1);
        return list;
    }

}
