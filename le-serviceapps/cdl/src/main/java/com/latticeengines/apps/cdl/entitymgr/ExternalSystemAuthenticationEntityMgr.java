package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;

public interface ExternalSystemAuthenticationEntityMgr {

    ExternalSystemAuthentication createAuthentication(ExternalSystemAuthentication externalSystemAuthentication);

    ExternalSystemAuthentication updateAuthentication(String authId, ExternalSystemAuthentication externalSystemAuthentication);

    List<ExternalSystemAuthentication> findAuthenticationsByLookupMapIds(List<String> lookupConfigIds);

    ExternalSystemAuthentication findAuthenticationByAuthId(String authId);

    List<ExternalSystemAuthentication> findAuthentications();

    List<ExternalSystemAuthentication> retrieveAuthenticationsByTrayAuthId(String trayAuthId);

}
