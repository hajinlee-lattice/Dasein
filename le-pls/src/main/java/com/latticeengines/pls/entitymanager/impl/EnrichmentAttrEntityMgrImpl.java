package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.pls.EnrichmentAttribute;
import com.latticeengines.pls.dao.EnrichmentAttrDao;
import com.latticeengines.pls.entitymanager.EnrichmentAttrEntityMgr;

@Component("enrichmentAttrEntityMgr")
public class EnrichmentAttrEntityMgrImpl implements EnrichmentAttrEntityMgr {

    @Autowired
    private EnrichmentAttrDao dao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<EnrichmentAttribute> findAll() {
        return dao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<EnrichmentAttribute> upsert(List<EnrichmentAttribute> newAttrs) {
        List<EnrichmentAttribute> oldAttrs = dao.findAll();
        for (EnrichmentAttribute attribute: oldAttrs) {
            if (!newAttrs.contains(attribute)) {
                dao.delete(attribute);
            }
        }
        for (EnrichmentAttribute attribute: newAttrs) {
            if (!oldAttrs.contains(attribute)) {
                dao.create(attribute);
            }
        }
        return dao.findAll();
    }

}
