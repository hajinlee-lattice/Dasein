package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.CustomerSourceAttributeDao;
import com.latticeengines.datacloud.core.dao.SourceAttributeDao;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.CustomerSourceAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

@Component("sourceAttributeEntityMgr")
public class SourceAttributeEntityMgrImpl implements SourceAttributeEntityMgr {

    @Autowired
    private SourceAttributeDao sourceAttributeDao;

    @Autowired
    private CustomerSourceAttributeDao customerSourceAttributeDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void createAttribute(SourceAttribute attr) {
        sourceAttributeDao.create(attr);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceAttribute> getAttributes(String source, String stage, String transform) {
        return sourceAttributeDao.getAttributes(source, stage, transform);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceAttribute> getAttributes(String source, String stage, String transform, String dataCloudVersion,
            boolean isCustomer) {
        if (!isCustomer) {
            return sourceAttributeDao.getAttributes(source, stage, transform, dataCloudVersion);
        } else {
            List<CustomerSourceAttribute> csaList = customerSourceAttributeDao.getAttributes(source, stage, transform,
                    dataCloudVersion);
            List<SourceAttribute> saList = new ArrayList<>();
            csaList.forEach(csa -> saList.add(csa.toSourceAttribute()));
            return saList;
        }

    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public String getLatestDataCloudVersion(String sourceName, String stage, String transformer) {
        return sourceAttributeDao.getLatestDataCloudVersion(sourceName, stage, transformer);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<String> getAllDataCloudVersions(String source, String stage, String transformer) {
        return sourceAttributeDao.getAllDataCloudVersions(source, stage, transformer);
    }

}
