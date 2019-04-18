package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.SalesforceURL;

public interface SalesforceURLDao extends BaseDao<SalesforceURL> {

    SalesforceURL findByURLName(String urlName);
}
