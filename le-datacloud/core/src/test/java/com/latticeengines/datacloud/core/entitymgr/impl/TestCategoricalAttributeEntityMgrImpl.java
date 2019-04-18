package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.CategoricalAttributeDao;
import com.latticeengines.datacloud.core.dao.CategoricalDimensionDao;
import com.latticeengines.datacloud.core.entitymgr.TestCategoricalAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;


@Component("testCategoricalAttributeEntityMgr")
public class TestCategoricalAttributeEntityMgrImpl  implements TestCategoricalAttributeEntityMgr {

    @Autowired
    private CategoricalAttributeDao attributeDao;

    @Autowired
    private CategoricalDimensionDao dimensionDao;

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public CategoricalDimension addDimension(CategoricalDimension dimension) {
        dimensionDao.create(dimension);
        CategoricalDimension dim = dimensionDao.findBySourceDimension(dimension.getSource(), dimension.getDimension());
        if (dim == null) {
            throw new RuntimeException(
                    "Failed to create dimension " + dimension.getSource() + ":" + dimension.getDimension());
        } else {
            return dim;
        }
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public CategoricalAttribute addAttribute(CategoricalAttribute attribute) {
        attributeDao.create(attribute);
        List<CategoricalAttribute> attrs = attributeDao.findAllByField("attrValue", attribute.getAttrValue());
        for (CategoricalAttribute attr : attrs) {
            if (attribute.getAttrName().equals(attr.getAttrName())) {
                if ((attribute.getParentId() == null && attr.getParentId() == null)
                        || (attribute.getParentId() != null && attribute.getParentId().equals(attr.getParentId()))) {
                    return attr;
                }
            }
        }
        throw new RuntimeException(
                "Failed to create attribute " + attribute.getAttrName() + ":" + attribute.getAttrValue());
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void removeAttribute(Long pid) {
        attributeDao.delete(attributeDao.findByKey(CategoricalAttribute.class, pid));
    }

    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW)
    public void removeDimension(Long pid) {
        dimensionDao.delete(dimensionDao.findByKey(CategoricalDimension.class, pid));
    }

    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<CategoricalAttribute> allAttributes() {
        List<CategoricalAttribute> attrs = attributeDao.findAll();
        if (attrs == null) {
            return Collections.emptyList();
        } else {
            return attrs;
        }
    }

    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<CategoricalDimension> allDimensions() {
        List<CategoricalDimension> dims = dimensionDao.findAll();
        if (dims == null) {
            return Collections.emptyList();
        } else {
            return dims;
        }
    }
}
