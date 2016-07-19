package com.latticeengines.playmaker.dao.impl;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class PlaymakerRecommendationDaoImplV710 extends PlaymakerRecommendationDaoImplV750 {

    public PlaymakerRecommendationDaoImplV710(NamedParameterJdbcTemplate namedJdbcTemplate) {
        super(namedJdbcTemplate);
    }

    protected String getAccountExtensionLastModificationDate() {
        return "E.[Last_Modification_Date] ";
    }
}
