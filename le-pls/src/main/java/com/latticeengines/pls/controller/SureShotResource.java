package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "sureshot", description = "REST resource for providing SureShot links")
@RestController
@RequestMapping(value = "/sureshot")
@PreAuthorize("hasRole('View_PLS_Configurations')")
public class SureShotResource {

    @Value("${pls.sureshot.map.creds.auth}")
    private String mapCredsAuthUrl;

    @Value("${pls.sureshot.scoring.settings}")
    private String scoringSettingsUrl;

    @Autowired
    private Oauth2AccessTokenEntityMgr oauth2AccessTokenEntityMgr;

    @RequestMapping(value = "/credentials", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Configure Credentials")
    @PreAuthorize("hasRole('Edit_PLS_Configurations')")
    public String getCredentialAuthenticationLink(@RequestParam(value = "crmType") String crmType,
            @RequestParam(value = "tenantId") String tenantId) {
        Oauth2AccessToken token = oauth2AccessTokenEntityMgr.get();
        return String.format(mapCredsAuthUrl + crmType + "?tenantId=%s&token=%s", tenantId, token.getAccessToken());
    }

    @RequestMapping(value = "/scoring/settings", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Configure Scoring Settings")
    @PreAuthorize("hasRole('Edit_PLS_Configurations')")
    public String getScoringSettingsLink(@RequestParam(value = "crmType") String crmType,
            @RequestParam(value = "tenantId") String tenantId) {
        Oauth2AccessToken token = oauth2AccessTokenEntityMgr.get();
        return String.format(scoringSettingsUrl + crmType + "?tenantId=%s&token=%s", tenantId, token.getAccessToken());
    }
}
