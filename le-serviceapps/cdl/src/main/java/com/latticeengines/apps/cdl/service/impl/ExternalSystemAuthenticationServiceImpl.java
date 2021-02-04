package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ExternalSystemAuthenticationEntityMgr;
import com.latticeengines.apps.cdl.service.ExternalSystemAuthenticationService;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;

@Component("externalSystemAuthenticationService")
public class ExternalSystemAuthenticationServiceImpl implements ExternalSystemAuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(ExternalSystemAuthenticationServiceImpl.class);

    @Inject
    private ExternalSystemAuthenticationEntityMgr externalSystemAuthenticationEntityMgr;

    @Override
    public ExternalSystemAuthentication createAuthentication(
            ExternalSystemAuthentication externalSystemAuthentication) {
        return externalSystemAuthenticationEntityMgr.createAuthentication(externalSystemAuthentication);
    }

    @Override
    public ExternalSystemAuthentication updateAuthentication(String authId,
            ExternalSystemAuthentication externalSystemAuthentication) {
        return externalSystemAuthenticationEntityMgr.updateAuthentication(authId, externalSystemAuthentication);
    }

    @Override
    public List<ExternalSystemAuthentication> findAuthenticationsByLookupMapIds(List<String> lookupConfigIds) {
        return externalSystemAuthenticationEntityMgr.findAuthenticationsByLookupMapIds(lookupConfigIds);
    }

    @Override
    public ExternalSystemAuthentication findAuthenticationByAuthId(String authId) {
        return externalSystemAuthenticationEntityMgr.findAuthenticationByAuthId(authId);
    }

    @Override
    public List<ExternalSystemAuthentication> findAuthentications() {
        return externalSystemAuthenticationEntityMgr.findAuthentications();
    }

    @Override
    public List<ExternalSystemAuthentication> findAuthenticationsByTrayAuthId(String trayAuthId) {
        return externalSystemAuthenticationEntityMgr.retrieveAuthenticationsByTrayAuthId(trayAuthId);
    }
}
