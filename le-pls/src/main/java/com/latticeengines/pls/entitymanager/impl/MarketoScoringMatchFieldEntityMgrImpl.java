package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.MarketoScoringMatchField;
import com.latticeengines.pls.dao.MarketoScoringMatchFieldDao;
import com.latticeengines.pls.entitymanager.MarketoScoringMatchFieldEntityMgr;
import com.latticeengines.pls.repository.reader.MarketoScoringMatchFieldReaderRepository;

@Component
public class MarketoScoringMatchFieldEntityMgrImpl extends BaseEntityMgrRepositoryImpl<MarketoScoringMatchField, Long> implements MarketoScoringMatchFieldEntityMgr {

    @Inject
    private MarketoScoringMatchFieldDao marketoScoringMatchFieldDao;
    
    @Inject
    private MarketoScoringMatchFieldReaderRepository marketoScoringMatchFieldReaderRepository;
    
    @Override
    public BaseDao<MarketoScoringMatchField> getDao() {
        return marketoScoringMatchFieldDao;
    }

    @Override
    public BaseJpaRepository<MarketoScoringMatchField, Long> getRepository() {
        return marketoScoringMatchFieldReaderRepository;
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Integer deleteFields(List<MarketoScoringMatchField> fields) {
        return marketoScoringMatchFieldDao.deleteFields(fields);
    }
}
