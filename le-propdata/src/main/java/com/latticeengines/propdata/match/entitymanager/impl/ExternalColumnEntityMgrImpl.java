package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.dao.ExternalColumnDao;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;

@Component("externalColumnEntityMgr")
public class ExternalColumnEntityMgrImpl implements MetadataColumnEntityMgr<ExternalColumn> {

    @Resource(name="externalColumnDao")
    private ExternalColumnDao externalColumnDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findByTag(String tag) {
        List<ExternalColumn> columns = getExternalColumnDao().findByTag(tag);
        List<ExternalColumn> toReturn = new ArrayList<>();
        for (ExternalColumn column: columns) {
            if (column.getTagList().contains(tag)) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    protected ExternalColumnDao getExternalColumnDao() {
        return externalColumnDao;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ExternalColumn> findAll() {
        return getExternalColumnDao().findAll();
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ExternalColumn findById(String externalColumnId) {
        return getExternalColumnDao().findByField("ExternalColumnID", externalColumnId);
    }

}
