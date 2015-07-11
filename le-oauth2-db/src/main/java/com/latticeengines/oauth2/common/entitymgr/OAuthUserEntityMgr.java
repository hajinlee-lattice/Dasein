package com.latticeengines.oauth2.common.entitymgr;

import com.latticeengines.domain.exposed.oauth.OAuthUser;

public interface OAuthUserEntityMgr {

    OAuthUser get(String userId);

    void create(OAuthUser user);

    void delete(String userId);

    void update(OAuthUser user);

    OAuthUser getByAccessToken(String token);

}