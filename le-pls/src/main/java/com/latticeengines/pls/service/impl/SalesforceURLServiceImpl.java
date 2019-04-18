package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SalesforceURL;
import com.latticeengines.pls.entitymanager.SalesforceURLEntityMgr;
import com.latticeengines.pls.service.SalesforceURLConstants;
import com.latticeengines.pls.service.SalesforceURLService;

@Component("salesforceURLService")
public class SalesforceURLServiceImpl implements SalesforceURLService {

    @Inject
    private SalesforceURLEntityMgr salesforceURLEntityMgr;

    @Override
    public String getBisLP() {
        return getURL(SalesforceURLConstants.BISLP_NAME);
    }

    @Override
    public String getBisLPSandbox() {
        String url = getURL(SalesforceURLConstants.BISLP_NAME);
        return getSandboxURL(url);
    }

    @Override
    public String getBisAP() {
        return getURL(SalesforceURLConstants.BISAP_NAME);
    }

    @Override
    public String getBisAPSandbox() {
        String url = getURL(SalesforceURLConstants.BISAP_NAME);
        return getSandboxURL(url);
    }

    private String getURL(String name) {
        SalesforceURL sfdcURL = salesforceURLEntityMgr.findByURLName(name);
        if (sfdcURL == null)
            throw new LedpException(LedpCode.LEDP_18039, new String[] { name });

        String url = sfdcURL.getURL();
        if (url == null || url.length() <= 0)
            throw new LedpException(LedpCode.LEDP_18040, new String[] { name });

        return url;
    }

    @Override
    public String getSandboxURL(String url) {
        return url.replaceAll("login.", "test.");
    }
}
