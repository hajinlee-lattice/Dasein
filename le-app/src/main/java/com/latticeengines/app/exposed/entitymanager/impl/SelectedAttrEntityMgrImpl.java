package com.latticeengines.app.exposed.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.app.exposed.dao.SelectedAttrDao;
import com.latticeengines.app.exposed.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;

@Component("enrichmentAttrEntityMgr")
public class SelectedAttrEntityMgrImpl implements SelectedAttrEntityMgr {

    @Inject
    private SelectedAttrDao dao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SelectedAttribute> findAll() {
        return dao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<SelectedAttribute> upsert(List<SelectedAttribute> newAttrList, //
            List<SelectedAttribute> dropAttrList) {
        add(newAttrList);
        return delete(dropAttrList);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<SelectedAttribute> add(List<SelectedAttribute> newAttrList) {
        List<SelectedAttribute> oldAttrs = dao.findAll();
        for (SelectedAttribute attribute : newAttrList) {
            if (!oldAttrs.contains(attribute)) {
                dao.create(attribute);
            }
        }
        return dao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<SelectedAttribute> delete(List<SelectedAttribute> dropAttrList) {
        List<SelectedAttribute> oldAttrs = dao.findAll();
        for (SelectedAttribute attribute : oldAttrs) {
            if (dropAttrList.contains(attribute)) {
                dao.delete(attribute);
            }
        }
        return dao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Integer count(boolean onlyPremium) {
        return dao.count(onlyPremium);
    }
}
