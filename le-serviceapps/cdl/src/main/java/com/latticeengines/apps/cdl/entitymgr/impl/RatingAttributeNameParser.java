package com.latticeengines.apps.cdl.entitymgr.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngine.ScoreType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component
public class RatingAttributeNameParser {

    public static final String SEPARATOR = "_";

    public Pair<ScoreType, String> parseTypeNMoelId(String type, String objId) {
        String modelId = objId.substring((BusinessEntity.Rating + ".").length());
        ScoreType scoreType = ScoreType.Rating;
        if (type.equals(VertexType.RATING_SCORE_ATTRIBUTE)) {
            scoreType = ScoreType.Score;
        } else if (type.equals(VertexType.RATING_PROB_ATTRIBUTE)) {
            scoreType = ScoreType.Probability;
        } else if (type.equals(VertexType.RATING_EV_ATTRIBUTE)) {
            scoreType = ScoreType.ExpectedRevenue;
        } else if (type.equals(VertexType.RATING_PREDICTED_REV_ATTRIBUTE)) {
            scoreType = ScoreType.PredictedRevenue;
        }

        if (scoreType != ScoreType.Rating) {
            modelId = modelId.substring(0, //
                    modelId.length() //
                            - suffix(scoreType).length());
        }
        Pair<ScoreType, String> pair = new ImmutablePair<>(scoreType, modelId);
        return pair;
    }

    public Pair<String, String> parseToTypeNModelId(String ratingAttribute) {
        String ratingAttrType = VertexType.RATING_ATTRIBUTE;
        String modelId = ratingAttribute;

        if (ratingAttribute.endsWith(suffix(ScoreType.Score))) {
            ratingAttrType = VertexType.RATING_SCORE_ATTRIBUTE;
            modelId = ratingAttribute.substring(0, //
                    ratingAttribute.length() //
                            - suffix(ScoreType.Score).length());
        } else if (ratingAttribute.endsWith(suffix(ScoreType.Probability))) {
            ratingAttrType = VertexType.RATING_PROB_ATTRIBUTE;
            modelId = ratingAttribute.substring(0, //
                    ratingAttribute.length() //
                            - (suffix(ScoreType.Probability)).length());
        } else if (ratingAttribute.endsWith(suffix(ScoreType.ExpectedRevenue))) {
            ratingAttrType = VertexType.RATING_EV_ATTRIBUTE;
            modelId = ratingAttribute.substring(0, //
                    ratingAttribute.length() //
                            - (suffix(ScoreType.ExpectedRevenue)).length());
        } else if (ratingAttribute.endsWith(suffix(ScoreType.PredictedRevenue))) {
            ratingAttrType = VertexType.RATING_PREDICTED_REV_ATTRIBUTE;
            modelId = ratingAttribute.substring(0, //
                    ratingAttribute.length() //
                            - (suffix(ScoreType.PredictedRevenue)).length());
        }

        return new ImmutablePair<>(ratingAttrType, modelId);
    }

    private String suffix(ScoreType type) {
        return SEPARATOR + RatingEngine.SCORE_ATTR_SUFFIX.get(type);
    }

}
