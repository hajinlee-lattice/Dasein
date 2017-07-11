package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.domain.exposed.security.Tenant;

public interface Oauth2AccessTokenEntityMgr {

    List<Oauth2AccessToken> findAll();

    void createOrUpdate(Oauth2AccessToken entity, String tenantId, String appId);

    Oauth2AccessToken get(String tenantId, String appId);

    Oauth2AccessToken findByTenant(Tenant tenant, String appId);

    void createOrUpdate(Oauth2AccessToken entity);
}
