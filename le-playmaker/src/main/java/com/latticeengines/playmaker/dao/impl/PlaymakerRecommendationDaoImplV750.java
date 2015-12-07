package com.latticeengines.playmaker.dao.impl;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class PlaymakerRecommendationDaoImplV750 extends PlaymakerRecommendationDaoImplV740 {

    public PlaymakerRecommendationDaoImplV750(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    @Override
    protected String getMonetaryValue() {
        return "L.[Monetary_Value] * L.[Likelihood] / 100 AS MonetaryValue, ";
    }
}
