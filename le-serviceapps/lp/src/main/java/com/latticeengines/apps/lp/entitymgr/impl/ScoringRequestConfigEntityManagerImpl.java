package com.latticeengines.apps.lp.entitymgr.impl;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.entitymgr.ScoringRequestConfigEntityManager;
import com.latticeengines.apps.lp.repository.reader.ScoringRequestConfigReaderRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

@Component("scoringRequestConfigEntityMgr")
public class ScoringRequestConfigEntityManagerImpl extends BaseEntityMgrRepositoryImpl<ScoringRequestConfig, Long>
        implements ScoringRequestConfigEntityManager {
    private static final Logger LOG = Logger.getLogger(ScoringRequestConfigEntityManagerImpl.class);

    @Inject
    private ScoringRequestConfigReaderRepository scoringRequestReadRepository;

    @Override
    public BaseJpaRepository<ScoringRequestConfig, Long> getRepository() {
        return scoringRequestReadRepository;
    }

    @Override
    public BaseDao<ScoringRequestConfig> getDao() {
        return new BaseDaoImpl<ScoringRequestConfig>() {
            @Override
            protected Class<ScoringRequestConfig> getEntityClass() {
                return ScoringRequestConfig.class;
            }
        };
    }

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        return scoringRequestReadRepository.retrieveScoringRequestConfigContext(configUuid);
    }

}
