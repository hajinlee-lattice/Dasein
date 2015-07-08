package com.latticeengines.oauth2.common.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2.common.dao.OAuthUserDao;
import com.latticeengines.oauth2.common.entitymgr.OAuthUserEntityMgr;

@Component("oAuthUserEntityMgr")
public class OAuthUserEntityMgrImpl implements OAuthUserEntityMgr {

    @Autowired
    private OAuthUserDao users;

    private PasswordEncoder encoder = new BCryptPasswordEncoder();

    @Override
    @Transactional(value = "oauth2")
    public OAuthUser get(String userId) {
        return users.get(userId);
    }

    @Override
    @Transactional(value = "oauth2")
    public void create(OAuthUser user) {
        if (user != null) {
            user = new OAuthUser(user);
            user.setPassword(encoder.encode(user.getPassword()));
        }
        users.create(user);
    }

    @Override
    @Transactional(value = "oauth2")
    public void delete(String userId) {
        users.delete(userId);
    }

    @Override
    @Transactional(value = "oauth2")
    public void update(OAuthUser user) {
        if (user != null) {
            user = new OAuthUser(user);
            user.setPassword(encoder.encode(user.getPassword()));
        }
        users.update(user);
    }

    @Override
    @Transactional(value = "oauth2")
    public OAuthUser getByAccessToken(String token) {
        OAuthUser user = users.getByAccessToken(token);
        return user;
    }

}
