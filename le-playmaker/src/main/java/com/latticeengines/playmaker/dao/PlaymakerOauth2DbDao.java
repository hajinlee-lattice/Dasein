package com.latticeengines.playmaker.dao;

import org.springframework.security.oauth2.provider.ClientDetails;

public interface PlaymakerOauth2DbDao {

    ClientDetails getClientByClientId(String clientId);

    void createClient(ClientDetails clientDetails);

    void deleteClientByClientId(String clientId);

    void updateClient(ClientDetails clientDetails);

    String findTenantByTokenId(String tokenId);

    void updateClientSecret(String clientId, String clientSecret);

}
