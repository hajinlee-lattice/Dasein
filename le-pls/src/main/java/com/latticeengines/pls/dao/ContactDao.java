package com.latticeengines.pls.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Contact;

public interface ContactDao extends BaseDao<Contact> {

    Contact findById(String contactId);
    List<Contact> findContacts(Map<String, String> params);
    Long findContactCount(Map<String, String> params);

}
