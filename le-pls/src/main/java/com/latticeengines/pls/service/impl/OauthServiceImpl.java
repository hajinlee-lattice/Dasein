package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.service.OauthService;
import com.latticeengines.pls.util.OauthRestApiProxy;

@Component("oauthService")
public class OauthServiceImpl implements OauthService {

    @Autowired
    private OauthRestApiProxy oauthRestApiProxy;

    @Override
    public String generateAPIToken(String tenantId) {
        return oauthRestApiProxy.generateAPIToken(tenantId);
    }

}
