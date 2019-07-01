package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.metadata.dao.AttributeDao;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.repository.db.AttributeRepository;

@Component("attributeEntityMgr")
public class AttributeEntityMgrImpl extends BaseEntityMgrRepositoryImpl<Attribute, Long> implements AttributeEntityMgr {

    @Autowired
    private AttributeDao attributeDao;

    @Autowired
    private AttributeRepository attributeRepository;

    @Override
    public BaseDao<Attribute> getDao() {
        return attributeDao;
    }

    @Override
    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Attribute> getAttributesByNamesAndTableName(List<String> attributeNames, String tableName,
                                                            int tableTypeCode) {
        return attributeRepository.getByNamesAndTableName(attributeNames, tableName, tableTypeCode);
    }

    @Override
    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public long countByTablePid(Long tablePid) {
        return attributeRepository.countByTable_Pid(tablePid);
    }

    @Override
    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Attribute> findByTablePid(Long tablePid) {
        return attributeRepository.findByTable_Pid(tablePid);
    }

    @Override
    @Transactional(transactionManager = "transactionManagerReader", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Attribute> findByTablePid(Long tablePid, Pageable pageable) {
        return attributeRepository.findByTable_Pid(tablePid, pageable);
    }

    @Override
    public BaseJpaRepository<Attribute, Long> getRepository() {
        return attributeRepository;
    }
}
