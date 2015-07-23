package com.latticeengines.upgrade.dl;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("dataLoaderManager")
public class DataLoaderManager {

    @Autowired
    private DataLoaderService dataLoaderService;

    private static String crmPassword = "";
    private static String crmSecurityToken = "security-token";

    @Value("${upgrade.dl.url1}")
    private String dlUrl1;

    @Value("${upgrade.dl.url2}")
    private String dlUrl2;

    @PostConstruct
    private void setCrmCredentials() throws Exception {
        crmPassword = CipherUtils.encrypt("password");
    }

    public SpaceConfiguration constructSpaceConfiguration(String tenantName) {
        String version = dataLoaderService.getTemplateVersion(tenantName, dlUrl1);
        if (!StringUtils.isEmpty(version)) {
            return constructSpaceConfiguration(dlUrl1, parseTopoloy(version));
        }

        version = dataLoaderService.getTemplateVersion(tenantName, dlUrl2);
        if (!StringUtils.isEmpty(version)) {
            return constructSpaceConfiguration(dlUrl2, parseTopoloy(version));
        }

        Exception e = new RuntimeException("Neither of the two prod dl urls can find the tenant requested, " +
                "or they both timeout.");
        throw new LedpException(LedpCode.LEDP_24000, e);

    }

    public CrmCredential constructCrmCredential(String tenantName, String dlUrl, CRMTopology topology) {
        if (topology.equals(CRMTopology.MARKETO)) {
            return constructMarketoCrmCredential(tenantName, dlUrl);
        }
        if (topology.equals(CRMTopology.ELOQUA)) {
            return constructEloquaCrmCredential(tenantName, dlUrl);
        }
        if (topology.equals(CRMTopology.SFDC)) {
            return constructSfdcCrmCredential(tenantName, dlUrl);
        }
        throw new UnsupportedOperationException("Unkown topology " + topology);
    }

    private CrmCredential constructMarketoCrmCredential(String tenantName, String dlUrl) {
        String userId = dataLoaderService.getMarketoUserId(tenantName, dlUrl);
        String url = dataLoaderService.getMarketoUrl(tenantName, dlUrl);
        if (StringUtils.isEmpty(userId) || StringUtils.isEmpty(url)) {
            return null;
        }
        return marketoCredential(userId, url);
    }

    private CrmCredential constructEloquaCrmCredential(String tenantName, String dlUrl) {
        String username = dataLoaderService.getEloquaUsername(tenantName, dlUrl);
        String company = dataLoaderService.getEloquaCompany(tenantName, dlUrl);
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(company)) {
            return null;
        }
        return eloquaCredential(username, company);
    }

    private CrmCredential constructSfdcCrmCredential(String tenantName, String dlUrl) {
        String user = dataLoaderService.getSfdcUser(tenantName, dlUrl);
        if (StringUtils.isEmpty(user)) {
            return null;
        }
        return sfdcCredential(user);
    }

    private static SpaceConfiguration constructSpaceConfiguration(String dlUrl, CRMTopology topology) {
        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        spaceConfiguration.setProduct(LatticeProduct.LPA);
        spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(topology);
        return spaceConfiguration;
    }

    private static CRMTopology parseTopoloy(String dlSpecVersion) {
        if (dlSpecVersion.contains("MKTO")) {
            return CRMTopology.MARKETO;
        }
        if (dlSpecVersion.contains("ELQ")) {
            return CRMTopology.ELOQUA;
        }
        Exception e = new RuntimeException("Unkown topology version " + dlSpecVersion);
        throw new LedpException(LedpCode.LEDP_24000, e);
    }

    private CrmCredential marketoCredential(String username, String url) {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setPassword(crmPassword);
        crmCredential.setUserName(username);
        crmCredential.setUrl(url);
        return crmCredential;
    }

    private CrmCredential eloquaCredential(String username, String company) {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setPassword(crmPassword);
        crmCredential.setUserName(username);
        crmCredential.setCompany(company);
        return crmCredential;
    }

    private CrmCredential sfdcCredential(String username) {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setPassword(crmPassword);
        crmCredential.setUserName(username);
        crmCredential.setSecurityToken(crmSecurityToken);
        return crmCredential;
    }

}
