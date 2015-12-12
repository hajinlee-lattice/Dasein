package com.latticeengines.pls.entitymanager.impl;

import java.util.Map;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Contact;
import com.latticeengines.pls.dao.ContactDao;
import com.latticeengines.pls.entitymanager.ContactEntityMgr;

@Component("contactEntityMgr")
public class ContactEntityMgrImpl extends BaseEntityMgrImpl<Contact> implements ContactEntityMgr {

    @Autowired
    private ContactDao contactDao;

    @Override
    public BaseDao<Contact> getDao() {
        return contactDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Contact findById(String contactId) {
        return contactDao.findById(contactId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Contact> findContacts(Map<String, String> params) {
        return contactDao.findContacts(params);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long findContactCount(Map<String, String> params) {
        return contactDao.findContactCount(params);
    }

}
