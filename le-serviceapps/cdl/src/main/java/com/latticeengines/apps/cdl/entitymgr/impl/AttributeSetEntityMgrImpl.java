package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AttributeSetDao;
import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.repository.AttributeSetRepository;
import com.latticeengines.apps.cdl.repository.reader.AttributeSetReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.AttributeSetWriterRepository;
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
    private AttributeSetReaderRepository attributeSetReaderRepository;

    @Inject
    private AttributeSetWriterRepository attributeSetWriterRepository;

    @Override
    protected AttributeSetRepository getReaderRepo() {
        return attributeSetReaderRepository;
    }

    @Override
    protected AttributeSetRepository getWriterRepo() {
        return attributeSetWriterRepository;
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
