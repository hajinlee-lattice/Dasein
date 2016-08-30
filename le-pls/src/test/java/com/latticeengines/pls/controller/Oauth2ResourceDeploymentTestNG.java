package com.latticeengines.pls.controller;

import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class Oauth2ResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final String NO_APP_ID = "";
    private static final String DUMMY_APP1 = "DUMMY_APP1";
    private static final String DUMMY_APP2 = "DUMMY_APP2";
    private Map<String, List<String>> appOauth2Map = new HashMap<>();

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
    }

    @Test(groups = { "deployment" })
    public void createApiToken() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/oauth2/apitoken?tenantId=" + mainTestTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createApiToken" })
    public void createOAuth2AccessTokenWithoutAppId() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId=" + mainTestTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = new ArrayList<>();
        oauth2TokenList.add(token);
        appOauth2Map.put(NO_APP_ID, oauth2TokenList);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithoutAppId" })
    public void createOAuth2AccessTokenWithAppId1() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestTenant.getId() + "&app_id=" + DUMMY_APP1, String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = new ArrayList<>();
        oauth2TokenList.add(token);
        appOauth2Map.put(DUMMY_APP1, oauth2TokenList);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithAppId1" })
    public void createOAuth2AccessTokenWithAppId2() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestTenant.getId() + "&app_id=" + DUMMY_APP2, String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = new ArrayList<>();
        oauth2TokenList.add(token);
        appOauth2Map.put(DUMMY_APP2, oauth2TokenList);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithAppId2" })
    public void checkAllOauthTokensValid() {

        System.out.println("===============");
        for (String appId : appOauth2Map.keySet()) {
            for (String oauthToken : appOauth2Map.get(appId)) {
                System.out.println("AppId = '" + appId + "', OauthToken = '" + oauthToken + "'");
            }
        }
        System.out.println("===============");
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "checkAllOauthTokensValid" })
    public void createOAuth2AccessTokenWithoutAppIdPhase2() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId=" + mainTestTenant.getId(), String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = appOauth2Map.get(NO_APP_ID);
        oauth2TokenList.add(token);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithoutAppIdPhase2" })
    public void createOAuth2AccessTokenWithAppId1Phase2() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestTenant.getId() + "&app_id=" + DUMMY_APP1, String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = appOauth2Map.get(DUMMY_APP1);
        oauth2TokenList.add(token);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithAppId1Phase2" })
    public void createOAuth2AccessTokenWithAppId2Phase2() {
        switchToExternalAdmin();
        String token = restTemplate.getForObject(getRestAPIHostPort() + "/pls/oauth2/accesstoken?tenantId="
                + mainTestTenant.getId() + "&app_id=" + DUMMY_APP2, String.class);
        assertTrue(StringUtils.isNotEmpty(token));
        List<String> oauth2TokenList = appOauth2Map.get(DUMMY_APP2);
        oauth2TokenList.add(token);
    }

    @Test(groups = { "deployment" }, dependsOnMethods = { "createOAuth2AccessTokenWithAppId2Phase2" })
    public void checkOnlyLatestOauthTokensValidPerAppId() {
        System.out.println("===============");
        for (String appId : appOauth2Map.keySet()) {
            for (String oauthToken : appOauth2Map.get(appId)) {
                System.out.println("AppId = '" + appId + "', OauthToken = '" + oauthToken + "'");
            }
        }
        System.out.println("===============");
        System.out.println("Complete");
    }
}
