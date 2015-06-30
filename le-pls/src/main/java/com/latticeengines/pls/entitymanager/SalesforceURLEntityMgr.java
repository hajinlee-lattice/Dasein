package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.SalesforceURL;

public interface SalesforceURLEntityMgr extends BaseEntityMgr<SalesforceURL> {

    SalesforceURL findByURLName(String urlName);
}
