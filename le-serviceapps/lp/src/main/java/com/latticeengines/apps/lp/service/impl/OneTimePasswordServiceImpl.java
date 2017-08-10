package com.latticeengines.apps.lp.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.lp.service.OneTimePasswordService;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@Service("oneTimePasswordService")
public class OneTimePasswordServiceImpl implements OneTimePasswordService {

    private static final Logger log = LoggerFactory.getLogger(OneTimePasswordServiceImpl.class);

    private final OAuthUserEntityMgr userEntityMgr;

    @Inject
    public OneTimePasswordServiceImpl(OAuthUserEntityMgr userEntityMgr) {
        this.userEntityMgr = userEntityMgr;
    }

    @Override
    public String generateOTP(String oauthUserId) {
        OAuthUser user = null;
        try {
            user = userEntityMgr.get(oauthUserId);
        } catch (Exception ex) {
            log.info("OAuth user does not exist! userId=" + oauthUserId);
        }
        if (user == null) {
            user = getNewOAuthUser(oauthUserId);
            userEntityMgr.create(user);
        } else {
            log.info("Generating new password for tenant " + oauthUserId);
            // Generate new password
            user.setPassword(OAuth2Utils.generatePassword());
            user.setPasswordExpiration(userEntityMgr.getPasswordExpiration(oauthUserId));
            user.setPasswordExpired(false);
            user.setUserId(oauthUserId);
            userEntityMgr.update(user);
        }
        log.info("Generated a one time password for userId=" + oauthUserId);
        return user.getPassword();
    }

    private OAuthUser getNewOAuthUser(String oauthUserId) {
        OAuthUser user = new OAuthUser();
        user.setUserId(oauthUserId);
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(userEntityMgr.getPasswordExpiration(oauthUserId));
        return user;
    }

}
