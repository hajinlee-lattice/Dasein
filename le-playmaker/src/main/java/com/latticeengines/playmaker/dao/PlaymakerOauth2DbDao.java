package com.latticeengines.playmaker.dao;

import com.latticeengines.playmaker.service.impl.ExtendedClientDetails;

public interface PlaymakerOauth2DbDao {

    ExtendedClientDetails getClientByClientId(String clientId);

    void createClient(ExtendedClientDetails clientDetails);

    void deleteClientByClientId(String clientId);

    void updateClient(ExtendedClientDetails clientDetails);

    String findTenantByTokenId(String tokenId);
}
