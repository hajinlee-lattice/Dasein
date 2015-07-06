package com.latticeengines.oauth2.common.dao.impl;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2.common.dao.OAuthUserDao;

public class OAuthUserDaoImpl extends BaseDaoImpl<OAuthUser> implements OAuthUserDao {

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

        String sql = "SELECT client_id FROM oauth_access_token WHERE token_id = :tokenId";
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
    public void delete(String userId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<OAuthUser> entityClz = getEntityClass();

        String queryStr = String.format("delete from %s where UserId = :userId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("userId", userId);
        query.executeUpdate();
    }

}