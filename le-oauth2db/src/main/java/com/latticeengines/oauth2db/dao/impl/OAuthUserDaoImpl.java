package com.latticeengines.oauth2db.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.store.JdbcTokenStore;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
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
        Query query = session.createQuery(queryStr);
        query.setString("userId", userId);
        query.setMaxResults(1);
        @SuppressWarnings("unchecked")
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
                app_id = auth.getOAuth2Request().getRequestParameters().get("app_id");
            } catch (Exception e) {
                log.error("Unable to find app_id in the authentication for token: " + token);
            }
        }
        return app_id;
    }

    @Override
    public void delete(String userId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OAuthUser> entityClz = getEntityClass();

        String queryStr = String.format("delete from %s where UserId = :userId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("userId", userId);
        query.executeUpdate();
    }

}