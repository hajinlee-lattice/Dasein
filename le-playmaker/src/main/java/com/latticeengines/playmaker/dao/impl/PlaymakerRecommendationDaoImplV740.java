package com.latticeengines.playmaker.dao.impl;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class PlaymakerRecommendationDaoImplV740 extends PlaymakerRecommendationDaoImpl {

    public PlaymakerRecommendationDaoImplV740(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    protected String getLikelihood() {
        return "CASE WHEN L.Normalized_Score IS NOT NULL THEN L.Normalized_Score ELSE L.Likelihood END AS Likelihood, ";
    }
}
