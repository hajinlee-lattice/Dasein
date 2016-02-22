package com.latticeengines.pls.entitymanager;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Contact;

public interface ContactEntityMgr extends BaseEntityMgr<Contact> {
    Contact findById(String contactID);
    List<Contact> findContacts(Map<String, String> params);
    Long findContactCount(Map<String, String> params);

}
