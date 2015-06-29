package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.provider.ClientDetails;
import org.springframework.security.oauth2.provider.client.BaseClientDetails;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.dao.PalymakerTenantDao;
import com.latticeengines.playmaker.dao.PlaymakerOauth2DbDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;

@Component("playmakerTenantEntityMgr")
public class PlaymakerTenantEntityMgrImpl implements PlaymakerTenantEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PalymakerTenantDao tenantDao;

    @Autowired
    private PlaymakerOauth2DbDao playmakerOauth2DbDao;

    @Override
    @Transactional(value = "playmaker")
    public void executeUpdate(PlaymakerTenant tenant) {
        tenantDao.update(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant create(PlaymakerTenant tenant) {

        PlaymakerTenant tenantInDb = tenantDao.findByTenantName(tenant.getTenantName());
        if (tenantInDb == null) {
            tenantDao.create(tenant);
        } else {
            tenantInDb.copyFrom(tenant);
            tenantDao.update(tenantInDb);
        }

        ClientDetails clientDetails = null;
        try {
            clientDetails = playmakerOauth2DbDao.getClientByClientId(tenant.getTenantName());
        } catch (Exception ex) {
            log.info("Client does not exist! client Id=" + tenant.getTenantName());
        }
        if (clientDetails == null) {
            clientDetails = getNewClientDetails(tenant);
            playmakerOauth2DbDao.createClient(clientDetails);
        } else {
            clientDetails = getNewClientDetails(tenant);
            playmakerOauth2DbDao.updateClient(clientDetails);
            playmakerOauth2DbDao.updateClientSecret(tenant.getTenantName(), getClientSecret());
        }
        tenant.setTenantPassword(clientDetails.getClientSecret());
        return tenant;
    }

    ClientDetails getNewClientDetails(PlaymakerTenant tenant) {
        BaseClientDetails clientDetails = new BaseClientDetails();
        clientDetails.setClientId(tenant.getTenantName());

        Set<GrantedAuthority> authorities = new HashSet<>();
        GrantedAuthority authority = new SimpleGrantedAuthority("PLAYMAKER_ROLE_CLIENT");
        authorities.add(authority);
        clientDetails.setAuthorities(authorities);

        List<String> grantedTypes = new ArrayList<>();
        grantedTypes.add("authorization_code");
        grantedTypes.add("refresh_token");
        grantedTypes.add("client_credentials");
        grantedTypes.add(TENANT_PASSWORD_KEY);
        clientDetails.setAuthorizedGrantTypes(grantedTypes);

        Set<String> scopes = new HashSet<>();
        scopes.add("read");
        clientDetails.setScope(scopes);

        Set<String> resourceIds = new HashSet<>();
        resourceIds.add("playmaker_api");
        clientDetails.setResourceIds(resourceIds);

        clientDetails.setClientSecret(getClientSecret());
        return clientDetails;
    }

    private String getClientSecret() {
        RandomValueStringGenerator generator = new RandomValueStringGenerator(12);
        return generator.generate();
    }

    @Override
    @Transactional(value = "playmaker")
    public void delete(PlaymakerTenant tenant) {
        tenantDao.deleteByTenantName(tenant.getTenantName());
        playmakerOauth2DbDao.deleteClientByClientId(tenant.getTenantName());
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByKey(PlaymakerTenant tenant) {
        return tenantDao.findByKey(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByTenantName(String tenantName) {
        PlaymakerTenant tenant = tenantDao.findByTenantName(tenantName);
        if (tenant != null) {
            ClientDetails clientDetails = null;
            try {
                clientDetails = playmakerOauth2DbDao.getClientByClientId(tenantName);
            } catch (Exception ex) {
            }
            if (clientDetails != null) {
                tenant.setTenantPassword(clientDetails.getClientSecret());
            } else {
                throw new LedpException(LedpCode.LEDP_22002, new String[] { tenantName });
            }
        }

        return tenant;
    }

    @Override
    @Transactional(value = "playmaker")
    public boolean deleteByTenantName(String tenantName) {
        return tenantDao.deleteByTenantName(tenantName);
    }

    @Override
    @Transactional(value = "playmaker")
    public void updateByTenantName(PlaymakerTenant tenant) {
        tenantDao.updateByTenantName(tenant);

    }

    @Override
    public String findTenantByTokenId(String tokenId) {
        return playmakerOauth2DbDao.findTenantByTokenId(tokenId);
    }
}
