package com.latticeengines.playmaker.dao.impl;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class PlaymakerRecommendationDaoImplV740 extends PlaymakerRecommendationDaoImpl {

    public PlaymakerRecommendationDaoImplV740(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    protected String getLikelihood() {
        return "COALESCE(L.Normalized_Score, L.Likelihood) AS Likelihood, ";
    }
}
