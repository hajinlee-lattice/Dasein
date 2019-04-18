package com.latticeengines.oauth2db.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.dao.OAuthUserDao;
import com.latticeengines.oauth2db.exposed.tokenstore.JsonJdbcTokenStore;

public class OAuthUserDaoImpl extends BaseDaoImpl<OAuthUser> implements OAuthUserDao {

    private static final Logger log = LoggerFactory.getLogger(OAuthUserDaoImpl.class);

    @Override
    protected Class<OAuthUser> getEntityClass() {
        return OAuthUser.class;
    }

    @Override
    public OAuthUser get(String userId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OAuthUser> entityClz = getEntityClass();

        String queryStr = String.format("from %s where UserId = :userId", entityClz.getSimpleName());
        Query<OAuthUser> query = session.createQuery(queryStr, OAuthUser.class);
        query.setParameter("userId", userId);
        query.setMaxResults(1);
        List<OAuthUser> list = query.list();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0);
        } else {
            return null;
        }
    }

    @Override
    public OAuthUser getByAccessToken(String token) {
        NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(getDataSource());

        String sql = "SELECT user_name FROM oauth_access_token WHERE token_id = :tokenId";
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("tokenId", token);

        String userId;
        try {
            userId = template.queryForObject(sql, params, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

        return get(userId);
    }

    @Override
    public String getAppIdByAccessToken(String token) {
        JdbcTokenStore tokenStore = new JsonJdbcTokenStore(getDataSource());
        String app_id = null;
        OAuth2Authentication auth = tokenStore.readAuthentication(token);
        if (auth != null) {
            try {
                app_id = auth.getOAuth2Request().getRequestParameters().get(CDLConstants.AUTH_APP_ID);
            } catch (Exception e) {
                log.error("Unable to find app_id in the authentication for token: " + token);
            }
        }
        return app_id;
    }

    @Override
    public Map<String, String> getOrgInfoByAccessToken(String token) {
        JdbcTokenStore tokenStore = new JsonJdbcTokenStore(getDataSource());
        Map<String, String> orgInfo = new HashMap<>();
        OAuth2Authentication auth = tokenStore.readAuthentication(token);
        if (auth != null) {
            try {

                if (auth.getOAuth2Request().getRequestParameters().containsKey(CDLConstants.ORG_ID) //
                        && auth.getOAuth2Request().getRequestParameters()
                                .containsKey(CDLConstants.EXTERNAL_SYSTEM_TYPE)) {
                    orgInfo = new HashMap<>();
                    orgInfo.put(CDLConstants.ORG_ID,
                            auth.getOAuth2Request().getRequestParameters().get(CDLConstants.ORG_ID));
                    orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE,
                            auth.getOAuth2Request().getRequestParameters().get(CDLConstants.EXTERNAL_SYSTEM_TYPE));
                }
            } catch (Exception e) {
                log.info("Unable to find orgIdKey, externalSystemTypeKey in the authentication for token: " + token);
            }
        }
        return orgInfo;

    }

    @Override
    public void delete(String userId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OAuthUser> entityClz = getEntityClass();

        String queryStr = String.format("delete from %s where UserId = :userId", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("userId", userId);
        query.executeUpdate();
    }

}
