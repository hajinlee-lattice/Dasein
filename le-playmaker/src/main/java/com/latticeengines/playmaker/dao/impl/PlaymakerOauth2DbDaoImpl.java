package com.latticeengines.playmaker.dao.impl;

import javax.sql.DataSource;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.oauth2.provider.ClientDetails;
import org.springframework.security.oauth2.provider.client.JdbcClientDetailsService;

import com.latticeengines.db.exposed.dao.impl.BaseGenericDaoImpl;
import com.latticeengines.playmaker.dao.PlaymakerOauth2DbDao;

public class PlaymakerOauth2DbDaoImpl extends BaseGenericDaoImpl implements PlaymakerOauth2DbDao {

    private JdbcClientDetailsService clientDetailService;

    public PlaymakerOauth2DbDaoImpl(DataSource dataSource) {
        super(new NamedParameterJdbcTemplate(dataSource));
        clientDetailService = new JdbcClientDetailsService(dataSource);
    }

    @Override
    public ClientDetails getClientByClientId(String clientId) {
        return clientDetailService.loadClientByClientId(clientId);
    }

    @Override
    public void createClient(ClientDetails clientDetails) {
        clientDetailService.addClientDetails(clientDetails);
    }

    @Override
    public void deleteClientByClientId(String clientId) {
        clientDetailService.removeClientDetails(clientId);
    }

    @Override
    public void updateClient(ClientDetails clientDetails) {
        clientDetailService.updateClientDetails(clientDetails);
    }

    @Override
    public void updateClientSecret(String clientId, String clientSecret) {
        clientDetailService.updateClientSecret(clientId, clientSecret);
    }

    @Override
    public String findTenantByTokenId(String tokenId) {
        String sql = "SELECT client_id FROM oauth_access_token WHERE token_id = :tokenId";
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("tokenId", tokenId);
        return queryForObject(sql, source, String.class);
    }
}
