package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.match.dao.ExternalColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

@Component("externalColumnEntityMgr")
public class ExternalColumnEntityMgrImpl implements MetadataColumnEntityMgr<ExternalColumn> {

    @Resource(name="externalColumnDao")
    private ExternalColumnDao externalColumnDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findByTag(String tag) {
        List<ExternalColumn> columns = externalColumnDao.findByTag(tag);
        List<ExternalColumn> toReturn = new ArrayList<>();
        for (ExternalColumn column: columns) {
            if (column.getTagList().contains(tag)) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findAll(String dataCloudVersion) {
        return externalColumnDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ExternalColumn findById(String externalColumnId) {
        return externalColumnDao.findByField("ExternalColumnID", externalColumnId);
    }

}
