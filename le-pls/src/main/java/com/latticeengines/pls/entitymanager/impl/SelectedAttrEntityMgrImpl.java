package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.pls.dao.SelectedAttrDao;
import com.latticeengines.pls.entitymanager.SelectedAttrEntityMgr;

@Component("enrichmentAttrEntityMgr")
public class SelectedAttrEntityMgrImpl implements SelectedAttrEntityMgr {

    @Autowired
    private SelectedAttrDao dao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<SelectedAttribute> findAll() {
        return dao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<SelectedAttribute> upsert(List<SelectedAttribute> newAttrs) {
        List<SelectedAttribute> oldAttrs = dao.findAll();
        for (SelectedAttribute attribute: oldAttrs) {
            if (!newAttrs.contains(attribute)) {
                dao.delete(attribute);
            }
        }
        for (SelectedAttribute attribute: newAttrs) {
            if (!oldAttrs.contains(attribute)) {
                dao.create(attribute);
            }
        }
        return dao.findAll();
    }

}
