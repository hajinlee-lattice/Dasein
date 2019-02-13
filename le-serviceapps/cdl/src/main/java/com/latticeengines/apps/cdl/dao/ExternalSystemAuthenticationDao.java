package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;

public interface ExternalSystemAuthenticationDao extends BaseDao<ExternalSystemAuthentication> {

    public ExternalSystemAuthentication updateAuthentication(ExternalSystemAuthentication extSysAuth);

}
