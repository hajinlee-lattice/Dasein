package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.pls.dao.Oauth2AccessTokenDao;

@Component("oauth2AccessTokenDao")
public class Oauth2AccessTokenDaoImpl extends BaseDaoImpl<Oauth2AccessToken> implements Oauth2AccessTokenDao {

    @Override
    protected Class<Oauth2AccessToken> getEntityClass() {
        return Oauth2AccessToken.class;
    }
}
