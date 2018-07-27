package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.pls.dao.ScoringRequestConfigDao;

@Component
public class ScoringRequestConfigDaoImpl extends BaseDaoImpl<ScoringRequestConfig> implements ScoringRequestConfigDao {

    @Override
    protected Class<ScoringRequestConfig> getEntityClass() {
        return ScoringRequestConfig.class;
    }
    
}
