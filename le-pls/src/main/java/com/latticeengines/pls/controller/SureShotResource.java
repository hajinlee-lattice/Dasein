package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.domain.exposed.pls.SureShotUrls;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "sureshot", description = "REST resource for providing SureShot links")
@RestController
@RequestMapping(value = "/sureshot")
public class SureShotResource {

    private static final Logger log = LoggerFactory.getLogger(SureShotResource.class);

    @Value("${pls.sureshot.map.creds.auth}")
    private String mapCredsAuthUrl;

    @Value("${pls.sureshot.scoring.settings}")
    private String scoringSettingsUrl;

    @Value("${pls.sureshot.enrichment.settings}")
    private String enrichmentSettingUrl;

    @Inject
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @RequestMapping(value = "/credentials", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Configure Credentials")
    public String getCredentialAuthenticationLink(@RequestParam(value = "crmType") String crmType,
            @RequestParam(value = CDLConstants.AUTH_APP_ID, required = false) String appId) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.error("Not able to get the tenant from SecurityContext");
            return null;
        }
        String tenantId = tenant.getId();
        Oauth2AccessToken token = oauth2AccessTokenEntityMgr.get(tenantId, appId);
        return String.format(mapCredsAuthUrl.replace("$$CRM_TYPE$$", crmType.toLowerCase()) + "?tenantId=%s&token=%s",
                tenantId, token.getAccessToken());
    }

    @RequestMapping(value = "/urls", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get SureShot Urls")
    public ResponseDocument<SureShotUrls> getSureShotUrls(@RequestParam(value = "crmType") String crmType,
            @RequestParam(value = CDLConstants.AUTH_APP_ID, required = false) String appId) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.error("Not able to get the tenant from SecurityContext");
            return ResponseDocument.failedResponse(new RuntimeException("Not able to get the tenant"));
        }
        String tenantId = tenant.getId();
        Oauth2AccessToken token = oauth2AccessTokenEntityMgr.get(tenantId, appId);
        String credsUrl = String
                .format(mapCredsAuthUrl.replace("$$CRM_TYPE$$", crmType.toLowerCase()) + "?tenantId=%s&token=%s",
                        tenantId, token.getAccessToken())
                .toString();
        String scoringUrl = String
                .format(scoringSettingsUrl.replace("$$CRM_TYPE$$", crmType.toLowerCase()) + "?tenantId=%s&token=%s",
                        tenantId, token.getAccessToken())
                .toString();
        String enrichmentUrl = String
                .format(enrichmentSettingUrl.replace("$$CRM_TYPE$$", crmType.toLowerCase()) + "?tenantId=%s&token=%s",
                        tenantId, token.getAccessToken())
                .toString();
        SureShotUrls urls = new SureShotUrls(credsUrl, scoringUrl, enrichmentUrl);
        return ResponseDocument.successResponse(urls);
    }
}
