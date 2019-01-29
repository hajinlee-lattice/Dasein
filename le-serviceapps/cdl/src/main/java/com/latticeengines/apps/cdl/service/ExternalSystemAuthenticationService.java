package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface ExternalSystemAuthenticationService {

    ExternalSystemAuthentication createAuthentication(ExternalSystemAuthentication externalSystemAuthentication);

    ExternalSystemAuthentication updateAuthentication(String authId, ExternalSystemAuthentication externalSystemAuthentication);

    List<ExternalSystemAuthentication> findAuthenticationsByLookupMapIds(List<String> lookupConfigIds);

    ExternalSystemAuthentication findAuthenticationByAuthId(String authId);

    List<ExternalSystemAuthentication> findAuthentications();

}
