package com.latticeengines.oauth2.db.dao;

import com.latticeengines.domain.exposed.oauth.OAuthUser;

public interface OAuthUserDao {

    OAuthUser get(String userId);

    void create(OAuthUser user);

    void delete(String userId);

    void update(OAuthUser user);

    OAuthUser getByAccessToken(String token);

}
