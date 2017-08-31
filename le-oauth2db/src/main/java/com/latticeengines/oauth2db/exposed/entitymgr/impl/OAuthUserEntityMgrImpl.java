package com.latticeengines.oauth2db.exposed.entitymgr.impl;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.dao.OAuthUserDao;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;

@Component("oAuthUserEntityMgr")
public class OAuthUserEntityMgrImpl implements OAuthUserEntityMgr {

    private final Logger log = LoggerFactory.getLogger(OAuthUserEntityMgrImpl.class);

    @Autowired
    private OAuthUserDao userDao;

    @Value("${oauth2.password_expiration_days}")
    private int passwordExpirationDays;

    private PasswordEncoder encoder = new BCryptPasswordEncoder();

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public OAuthUser get(String userId) {
        return userDao.get(userId);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void create(OAuthUser user) {
        user.setEncryptedPassword(encoder.encode(user.getPassword()));
        userDao.create(user);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void delete(String userId) {
        userDao.delete(userId);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void update(OAuthUser user) {
        if (user.getPassword() != null) {
            user.setEncryptedPassword(encoder.encode(user.getPassword()));
        }
        userDao.update(user);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public OAuthUser getByAccessToken(String token) {
        return userDao.getByAccessToken(token);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public String findTenantNameByAccessToken(String accessToken) {
        OAuthUser user = getByAccessToken(accessToken);
        if (user != null) {
            return user.getUserId();
        }
        return null;
    }

    @Override
    public Date getPasswordExpiration(String userId) {
        if (passwordExpirationDays <= 0) {
            log.info(String.format("oauth2.password_expiration_days <= 0.  Disabling expiration for user with id=%s",
                    userId));
            return null;
        }
        return DateTime.now(DateTimeZone.UTC).plusDays(passwordExpirationDays).toDate();
    }

}
