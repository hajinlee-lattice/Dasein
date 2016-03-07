package com.latticeengines.pls.entitymanager;

import java.util.List;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;

public interface Oauth2AccessTokenEntityMgr {

    List<Oauth2AccessToken> findAll();

    void createOrUpdate(Oauth2AccessToken entity);

    Oauth2AccessToken get();
}
