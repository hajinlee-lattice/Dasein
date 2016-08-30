package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;

public interface Oauth2AccessTokenDao extends BaseDao<Oauth2AccessToken> {
    Oauth2AccessToken findByTenantAppId(Long tenantId, String appId);
}
