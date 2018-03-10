package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

public enum RatingEngineDependencyType {
    Segment("Segment"), //
    Model("Model"), // i.e. RatingEngine
    Play("Play"); //

    private String ratingEngineDependencyType;
    private static Map<String, RatingEngineDependencyType> map = new HashMap<>();

    static {
        for (RatingEngineDependencyType r : RatingEngineDependencyType.values()) {
            map.put(r.getRatingEngineDependencyType(), r);
        }
    }

    RatingEngineDependencyType(String ratingEngineDependencyType) {
        this.ratingEngineDependencyType = ratingEngineDependencyType;
    }

    public String getRatingEngineDependencyType() {
        return ratingEngineDependencyType;
    }

    public static RatingEngineDependencyType getByRatingEngineDependencyType(String ratingEngineDependencyType) {
        return map.get(ratingEngineDependencyType);
    }
}
