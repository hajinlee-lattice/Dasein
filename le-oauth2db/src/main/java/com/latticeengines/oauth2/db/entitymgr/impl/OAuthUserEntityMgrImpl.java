package com.latticeengines.oauth2.db.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2.db.dao.OAuthUserDao;
import com.latticeengines.oauth2.db.entitymgr.OAuthUserEntityMgr;

@Component("oAuthUserEntityMgr")
public class OAuthUserEntityMgrImpl implements OAuthUserEntityMgr {

    @Autowired
    private OAuthUserDao userDao;

    private PasswordEncoder encoder = new BCryptPasswordEncoder();

    @Override
    @Transactional(value = "oauth2")
    public OAuthUser get(String userId) {
        return userDao.get(userId);
    }

    @Override
    @Transactional(value = "oauth2")
    public void create(OAuthUser user) {
        user.setEncryptedPassword(encoder.encode(user.getPassword()));
        userDao.create(user);
    }

    @Override
    @Transactional(value = "oauth2")
    public void delete(String userId) {
        userDao.delete(userId);
    }

    @Override
    @Transactional(value = "oauth2")
    public void update(OAuthUser user) {
        if (user.getPassword() != null) {
            user.setEncryptedPassword(encoder.encode(user.getPassword()));
        }
        userDao.update(user);
    }

    @Override
    @Transactional(value = "oauth2")
    public OAuthUser getByAccessToken(String token) {
        OAuthUser user = userDao.getByAccessToken(token);
        return user;
    }

}
