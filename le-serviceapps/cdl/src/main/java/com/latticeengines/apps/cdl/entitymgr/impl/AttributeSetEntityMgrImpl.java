package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AttributeSetDao;
import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.repository.AttributeSetRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.AttributeSet;

@Component("attributeSetEntityMgr")
public class AttributeSetEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<AttributeSetRepository, AttributeSet, Long>
        implements AttributeSetEntityMgr {

    @Inject
    private AttributeSetEntityMgrImpl _self;

    @Inject
    private AttributeSetDao attributeSetDao;

    @Inject
    private AttributeSetRepository readerRepository;

    @Inject
    private AttributeSetRepository writerRepository;

    @Override
    protected AttributeSetRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected AttributeSetRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<AttributeSetRepository, AttributeSet, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<AttributeSet> getDao() {
        return attributeSetDao;
    }

}
