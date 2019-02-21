package com.latticeengines.playmaker.controller;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.functionalframework.PlaymakerTestNGBase;

public class RecommendationResourceDeploymentTestNG extends PlaymakerTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(RecommendationResourceDeploymentTestNG.class);

    private OAuth2RestTemplate restTemplate = null;

    private PlaymakerTenant newTenant = null;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        super.beforeClass();
        SSLUtils.turnOffSSLNameVerification();
        newTenant = playMakerEntityMgr.create(tenant);
        restTemplate = createOAuth2RestTemplate(newTenant, "playmaker");
    }

    @Test(groups = "deployment")
    public void getRecommendations() {
        testGetRecommendations(restTemplate);
    }


    private void testGetRecommendations(OAuth2RestTemplate authRestTemplate) {
        String url = apiHostPort + "/playmaker/recommendations?start=1&offset=1&maximum=100&destination=SFDC";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = authRestTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        System.out.println(result.toString());
    }

    @Test(groups = "deployment")
    public void getRecommendationCount() {
        String url = apiHostPort + "/playmaker/recommendationcount?start=1&destination=SFDC";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getPlays() {
        String url = apiHostPort + "/playmaker/plays?start=1&offset=1&maximum=100&destination=SFDC";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getPlayCount() {
        String url = apiHostPort + "/playmaker/playcount?start=1&destination=SFDC";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensions() {
        getAccountExtensions(null);
        getAccountExtensions("Recommendations");
        getAccountExtensions("NoRecommendations");

        testEmptyResultWithLargeOffset(false, null);
        testEmptyResultWithLargeOffset(true, null);
        testEmptyResultWithLargeOffset(false, "BAD_COLUMN");
        testEmptyResultWithLargeOffset(false, "CrmRefreshDate");
        testEmptyResultWithLargeOffset(false, "CrmRefreshDate,RevenueGrowth,BAD_COLUMN");
    }

    private void getAccountExtensions(String filterBy) {
        getAccountExtensions(false, null, filterBy, false);
        getAccountExtensions(true, null, filterBy, true);
        getAccountExtensions(false, "BAD_COLUMN", filterBy, true);
        getAccountExtensions(false, "CrmRefreshDate", filterBy, false);
        getAccountExtensions(false, "CrmRefreshDate,RevenueGrowth,BAD_COLUMN", filterBy, false);
    }

    private void getAccountExtensions(boolean shouldSendEmptyColumnMapping, String columns, String filterBy,
            boolean expectColumnsSizeEqualTo6) {
        int offset = 0;
        String url = apiHostPort + "/playmaker/%s?start=1&offset=" + offset + "&maximum=250";
        if (columns == null) {
            if (shouldSendEmptyColumnMapping) {
                url += "&columns=";
            }
        } else {
            url += "&columns=" + columns;
        }
        if (filterBy != null) {
            url += "&filterBy=" + filterBy;
            url += "&recStart=1";
        }

        String countUrl = String.format(url, "accountextensioncount");
        @SuppressWarnings("unchecked")
        Map<String, Object> countResult = restTemplate.getForObject(countUrl, Map.class);
        Assert.assertNotNull(countResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY));
        Integer totalCount = (Integer) countResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(totalCount > 0);

        url = String.format(url, "accountextensions");

        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey("startDatetime"));
        Assert.assertTrue(result.containsKey("endDatetime"));
        Assert.assertTrue(result.containsKey("records"));
        @SuppressWarnings("unchecked")
        List<Map<String, String>> records = (List<Map<String, String>>) result.get("records");
        Assert.assertNotNull(records);
        Assert.assertEquals(new Integer(records.size()), totalCount);
        List<String> impFields = Arrays.asList("ID", "SfdcAccountID", "LEAccountExternalID", "LastModificationDate",
                "RowNum");
        List<Class<?>> impFieldTypes = Arrays.asList(Long.class, String.class, String.class, Long.class, Long.class);

        int rowCount = 0;
        for (Map<String, String> rec : records) {
            rowCount++;
            int idx = 0;
            for (String field : impFields) {
                Class<?> type = impFieldTypes.get(idx++);
                Assert.assertTrue(rec.containsKey(field));
                Assert.assertNotNull(rec.get(field));
                if (type == Long.class) {
                    Object valObj = rec.get(field);
                    Long val = Long.parseLong(valObj.toString());
                    if (field.equals("RowNum")) {
                        Assert.assertEquals(val, new Long((rowCount + offset)));
                    }
                }
            }
            Assert.assertTrue(rec.containsKey("SfdcContactID"));

            if (expectColumnsSizeEqualTo6) {
                Assert.assertTrue(rec.size() == 6,
                        String.format("rec.size() = %d, expected to be = %d", rec.size(), 6));
            } else {
                Assert.assertTrue(rec.size() > 6, String.format("rec.size() = %d, expected to be > %d", rec.size(), 6));
            }

        }
    }

    private void testEmptyResultWithLargeOffset(boolean shouldSendEmptyColumnMapping, String columns) {
        int offset = 1000000;
        String url = apiHostPort + "/playmaker/accountextensions?start=1&offset=" + offset + "&maximum=250";
        if (columns == null) {
            if (shouldSendEmptyColumnMapping) {
                url += "&columns=";
            }
        } else {
            url += "&columns=" + columns;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() == 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionCount() {
        String url = apiHostPort + "/playmaker/accountextensioncount?start=1";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionSchema() {
        String url = apiHostPort + "/playmaker/accountextensionschema";
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionColumnCount() {
        String url = apiHostPort + "/playmaker/accountextensioncolumncount";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getContacts() {
        String url = apiHostPort + "/playmaker/contacts?start=1&offset=1&maximum=100";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getContactCount() {
        String url = apiHostPort + "/playmaker/contactcount?start=1";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getContactExtensions() {
        String url = apiHostPort + "/playmaker/contactextensions?start=1&offset=1&maximum=100";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getContactExtensionCount() {
        String url = apiHostPort + "/playmaker/contactextensioncount?start=1";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getContactExtensionSchema() {
        String url = apiHostPort + "/playmaker/contactextensionschema";
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getContactExtensionColumnCount() {
        String url = apiHostPort + "/playmaker/contactextensioncolumncount";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValues() {
        String url = apiHostPort + "/playmaker/playvalues?start=1&offset=1&maximum=100";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValueCount() {
        String url = apiHostPort + "/playmaker/playvaluecount?start=1";
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getWorkflowTypes() {
        String url = apiHostPort + "/playmaker/workflowtypes";
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayGroups() {
        String url = apiHostPort + "/playmaker/playgroups?start=1&offset=1&maximum=100";
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = "getPlayGroups")
    public void getOauthTokenToTenant() {
        String url = apiHostPort + "/playmaker/oauthtotenant";
        String tenantNameViaToken = restTemplate.getForObject(url, String.class);
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, newTenant.getTenantName());
    }

    @Test(groups = "deployment", dependsOnMethods = "getOauthTokenToTenant")
    public void createPrereqForRecommendations() {
        String url = apiHostPort + "/playmaker/oauthtotenant";
        String tenantNameViaToken = restTemplate.getForObject(url, String.class);
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, newTenant.getTenantName());
    }

    @AfterClass(groups = "deployment")
    public void afterClass() {
        tryGettingRecommendationsWithLPOauthToken();
        playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
        SSLUtils.turnOnSSLNameVerification();
    }

    private void tryGettingRecommendationsWithLPOauthToken() {
        PlaymakerTenant tenant = getTenant();
        tenant = playMakerEntityMgr.create(tenant);

        OAuth2RestTemplate lpRestTemplate = createOAuth2RestTemplate(tenant, "lp");

        testGetRecommendations(lpRestTemplate);
        playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
    }

    private OAuth2RestTemplate createOAuth2RestTemplate(PlaymakerTenant tenant, String clientId) {
        OAuth2RestTemplate lpRestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenant.getTenantName(),
                tenant.getTenantPassword(), clientId);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(lpRestTemplate);
        log.info(String.format("Oauth access token = %s, client id = %s", accessToken.getValue(), clientId));
        return lpRestTemplate;
    }
}
