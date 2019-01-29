package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExternalSystemAuthenticationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;

@Component("externalSystemAuthenticationDao")
public class ExternalSystemAuthenticationDaoImpl extends BaseDaoImpl<ExternalSystemAuthentication> implements ExternalSystemAuthenticationDao {

    @Override
    protected Class<ExternalSystemAuthentication> getEntityClass() {
        return ExternalSystemAuthentication.class;
    }

}
