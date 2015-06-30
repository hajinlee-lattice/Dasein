package com.latticeengines.oauth2.common.dao.impl;

import java.util.Date;

import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.oauth2.provider.client.BaseClientDetails;
import org.springframework.security.oauth2.provider.client.JdbcClientDetailsService;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.oauth2.common.dao.OAuthClientDao;
import com.latticeengines.oauth2.common.service.ExtendedClientDetails;
import com.latticeengines.oauth2.common.service.impl.OAuthClientDetails;

public class OAuthClientDaoImpl extends BaseGenericDaoImpl implements OAuthClientDao {

    private JdbcClientDetailsService clientDetailService;

    public OAuthClientDaoImpl(DataSource dataSource) {
        super(new NamedParameterJdbcTemplate(dataSource));
        clientDetailService = new JdbcClientDetailsService(dataSource);
    }

    @Override
    public ExtendedClientDetails getClientByClientId(String clientId) {
        BaseClientDetails details = (BaseClientDetails) clientDetailService.loadClientByClientId(clientId);
        return loadExtendedColumns(details);
    }

    @Override
    public void createClient(ExtendedClientDetails clientDetails) {
        clientDetailService.addClientDetails(clientDetails);
        updateExtendedColumns(clientDetails);
    }

    @Override
    public void deleteClientByClientId(String clientId) {
        clientDetailService.removeClientDetails(clientId);
    }

    @Override
    public void updateClient(ExtendedClientDetails clientDetails) {
        clientDetailService.updateClientDetails(clientDetails);
        clientDetailService.updateClientSecret(clientDetails.getClientId(), clientDetails.getClientSecret());
        updateExtendedColumns(clientDetails);
    }

    @Override
    public String findTenantByTokenId(String tokenId) {
        String sql = "SELECT client_id FROM oauth_access_token WHERE token_id = :tokenId";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("tokenId", tokenId);
        return queryForObject(sql, source, String.class);
    }

    private ExtendedClientDetails loadExtendedColumns(BaseClientDetails clientDetails) {
        OAuthClientDetails toReturn = new OAuthClientDetails(clientDetails);

        String sql = "SELECT client_secret_expiration FROM oauth_client_details WHERE client_id = :clientId";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("clientId", clientDetails.getClientId());

        Date expiration = queryForObject(sql, source, Date.class);

        toReturn.setClientSecretExpiration(new DateTime(expiration));
        return toReturn;
    }

    private void updateExtendedColumns(ExtendedClientDetails clientDetails) {
        String sql = "UPDATE oauth_client_details SET client_secret_expiration = :expiration WHERE client_id = :clientId";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("clientId", clientDetails.getClientId());
        source.addValue("expiration", clientDetails.getClientSecretExpiration().toDate());
        update(sql, source);
    }
}
