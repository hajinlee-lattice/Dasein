package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.CategoricalAttributeDao;
import com.latticeengines.datacloud.core.dao.CategoricalDimensionDao;
import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

@Component("categoricalAttributeEntityMgrImpl")
public class CategoricalAttributeEntityMgrImpl implements CategoricalAttributeEntityMgr {
    @Autowired
    private CategoricalAttributeDao attributeDao;

    @Autowired
    private CategoricalDimensionDao dimensionDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<CategoricalAttribute> getChildren(Long parentId) {
        return attributeDao.findAllByField("parentId", parentId);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public CategoricalAttribute getRootAttribute(String source, String dimension) {
        CategoricalDimension dim = dimensionDao.findBySourceDimension(source, dimension);
        Long rootAttrId = dim.getRootAttrId();
        CategoricalAttribute rootAttr = attributeDao.findByKey(CategoricalAttribute.class, rootAttrId);
        return rootAttr;
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public CategoricalAttribute getAttribute(Long pid) {
        return attributeDao.findByKey(CategoricalAttribute.class, pid);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public CategoricalAttribute getAttribute(String attrName, String attrValue) {
        if (StringUtils.isNotEmpty(attrValue)) {
            return attributeDao.findByNameValue(attrName, attrValue);
        } else {
            return null;
        }
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<CategoricalDimension> getAllDimensions() {
        return dimensionDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<CategoricalAttribute> getAllAttributes(Long parentId) {
        CategoricalAttribute parentAttribute = getAttribute(parentId);
        List<CategoricalAttribute> childAttributes = getChildren(parentAttribute.getPid());
        if (CollectionUtils.isEmpty(childAttributes)) {
            childAttributes = new ArrayList<CategoricalAttribute>();
        }
        childAttributes.add(0, parentAttribute);
        return childAttributes;
    }

}
