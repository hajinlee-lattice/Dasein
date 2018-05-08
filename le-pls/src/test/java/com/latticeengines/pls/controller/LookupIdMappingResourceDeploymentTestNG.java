package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class LookupIdMappingResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private String orgId = "Org_" + System.currentTimeMillis();
    private String orgName = "Dummy name";
    private CDLExternalSystemType externalSystemType = CDLExternalSystemType.CRM;
    private String configId = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        switchToSuperAdmin();

    }

    @Test(groups = "deployment")
    public void getLookupIdsMapping() {
        @SuppressWarnings({ "rawtypes" })
        Map lookupIdConfigsRaw = (Map) restTemplate.getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping",
                Map.class);
        Assert.assertNotNull(lookupIdConfigsRaw);
        @SuppressWarnings({ "unchecked" })
        Map<String, List<LookupIdMap>> lookupIdConfigs = JsonUtils.convertMapWithListValue(lookupIdConfigsRaw,
                String.class, LookupIdMap.class);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() == 0);
    }

    @Test(groups = "deployment")
    public void registerExternalSystem() {
        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId(orgId);
        lookupIdsMap.setOrgName(orgName);
        lookupIdsMap.setExternalSystemType(externalSystemType);

        LookupIdMap resultLookupIdMap = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/lookup-id-mapping/register", lookupIdsMap, LookupIdMap.class);
        Assert.assertNotNull(resultLookupIdMap);
        Assert.assertEquals(resultLookupIdMap.getOrgId(), lookupIdsMap.getOrgId());
        Assert.assertEquals(resultLookupIdMap.getOrgName(), lookupIdsMap.getOrgName());
        Assert.assertEquals(resultLookupIdMap.getExternalSystemType(), lookupIdsMap.getExternalSystemType());
        Assert.assertNotNull(resultLookupIdMap.getId());
        Assert.assertNotNull(resultLookupIdMap.getIsRegistered());
        Assert.assertEquals(resultLookupIdMap.getIsRegistered(), Boolean.TRUE);

        configId = resultLookupIdMap.getId();

        confirmNonEmptyLookupConfigs(Boolean.TRUE);
    }

    @Test(groups = "deployment")
    public void updateLookupIdMap() {
        @SuppressWarnings({ "rawtypes" })
        Map lookupIdConfigsRaw = (Map) restTemplate.getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping",
                Map.class);
        Assert.assertNotNull(lookupIdConfigsRaw);
        @SuppressWarnings({ "unchecked" })
        Map<String, List<LookupIdMap>> lookupIdConfigs = JsonUtils.convertMapWithListValue(lookupIdConfigsRaw,
                String.class, LookupIdMap.class);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().stream().forEach(k -> {
            Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
            lookupIdConfigs.get(k).stream().forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getId());
                Assert.assertNotNull(c.getOrgId());
                Assert.assertNotNull(c.getOrgName());
                Assert.assertNotNull(c.getIsRegistered());
                Assert.assertEquals(c.getIsRegistered(), Boolean.TRUE);
                c.setAccountId("Acc_" + System.currentTimeMillis());

                restTemplate.put(getRestAPIHostPort() + "/pls/lookup-id-mapping/config/" + c.getId(), c,
                        LookupIdMap.class);
            });
        });

    }

    @Test(groups = "deployment")
    public void testDeregisterExternalSystem() {
        LookupIdMap configBeforeDeregister = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping/config/" + configId, LookupIdMap.class);
        Assert.assertEquals(configBeforeDeregister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configBeforeDeregister.getId(), configId);
        Assert.assertEquals(configBeforeDeregister.getOrgId(), orgId);
        Assert.assertEquals(configBeforeDeregister.getOrgName(), orgName);
        Assert.assertEquals(configBeforeDeregister.getIsRegistered(), Boolean.TRUE);

        LookupIdMap lookupIdsMap = new LookupIdMap();
        lookupIdsMap.setOrgId(orgId);
        lookupIdsMap.setOrgName(orgName);
        lookupIdsMap.setExternalSystemType(externalSystemType);

        restTemplate.put(getRestAPIHostPort() + "/pls/lookup-id-mapping/deregister", lookupIdsMap, LookupIdMap.class);

        LookupIdMap configAfterDeregister = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping/config/" + configId, LookupIdMap.class);
        Assert.assertEquals(configAfterDeregister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configAfterDeregister.getId(), configId);
        Assert.assertEquals(configAfterDeregister.getOrgId(), orgId);
        Assert.assertEquals(configAfterDeregister.getOrgName(), orgName);
        Assert.assertEquals(configAfterDeregister.getIsRegistered(), Boolean.FALSE);

        confirmNonEmptyLookupConfigs(Boolean.FALSE);

        LookupIdMap resultLookupIdMap = restTemplate.postForObject(
                getRestAPIHostPort() + "/pls/lookup-id-mapping/register", lookupIdsMap, LookupIdMap.class);
        Assert.assertNotNull(resultLookupIdMap);
        Assert.assertEquals(resultLookupIdMap.getOrgId(), lookupIdsMap.getOrgId());
        Assert.assertEquals(resultLookupIdMap.getOrgName(), lookupIdsMap.getOrgName());
        Assert.assertEquals(resultLookupIdMap.getExternalSystemType(), lookupIdsMap.getExternalSystemType());
        Assert.assertNotNull(resultLookupIdMap.getId());
        Assert.assertNotNull(resultLookupIdMap.getIsRegistered());
        Assert.assertEquals(resultLookupIdMap.getIsRegistered(), Boolean.TRUE);

        LookupIdMap configAfterAnotherRegister = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping/config/" + configId, LookupIdMap.class);
        Assert.assertEquals(configAfterAnotherRegister.getExternalSystemType(), externalSystemType);
        Assert.assertEquals(configAfterAnotherRegister.getId(), configId);
        Assert.assertEquals(configAfterAnotherRegister.getOrgId(), orgId);
        Assert.assertEquals(configAfterAnotherRegister.getOrgName(), orgName);
        Assert.assertEquals(configAfterAnotherRegister.getIsRegistered(), Boolean.TRUE);

        confirmNonEmptyLookupConfigs(Boolean.TRUE);
    }

    @Test(groups = "deployment")
    public void getAllLookupIds() {

        @SuppressWarnings({ "rawtypes" })
        Map allLookupIdsRaw = (Map) restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping/available-lookup-ids", Map.class);
        Assert.assertNotNull(allLookupIdsRaw);
        @SuppressWarnings({ "unchecked" })
        Map<String, List<CDLExternalSystemMapping>> allLookupIds = JsonUtils.convertMapWithListValue(allLookupIdsRaw,
                String.class, CDLExternalSystemMapping.class);
        Assert.assertNotNull(allLookupIds);
        Assert.assertTrue(allLookupIds.keySet().size() > 0);

        allLookupIds.keySet().stream().forEach(k -> {
            CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
            Assert.assertNotNull(externalSystemType);
            Assert.assertTrue(allLookupIds.get(k).size() > 0);
            allLookupIds.get(k).stream().forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertNotNull(c.getDisplayName());
                Assert.assertNotNull(c.getFieldName());
                Assert.assertNotNull(c.getFieldType());
            });
        });

    }

    @Test(groups = "deployment")
    public void getAllCDLExternalSystemType() {
        List<?> allCDLExternalSystemTypeRaw = restTemplate
                .getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping/all-external-system-types", List.class);
        Assert.assertNotNull(allCDLExternalSystemTypeRaw);

        List<CDLExternalSystemType> allCDLExternalSystemType = JsonUtils.convertList(allCDLExternalSystemTypeRaw,
                CDLExternalSystemType.class);
        Assert.assertNotNull(allCDLExternalSystemType);
        Assert.assertTrue(CollectionUtils.isNotEmpty(allCDLExternalSystemType));
    }

    private void confirmNonEmptyLookupConfigs(Boolean isMarkedRegistered) {
        @SuppressWarnings({ "rawtypes" })
        Map lookupIdConfigsRaw = (Map) restTemplate.getForObject(getRestAPIHostPort() + "/pls/lookup-id-mapping",
                Map.class);
        Assert.assertNotNull(lookupIdConfigsRaw);
        @SuppressWarnings({ "unchecked" })
        Map<String, List<LookupIdMap>> lookupIdConfigs = JsonUtils.convertMapWithListValue(lookupIdConfigsRaw,
                String.class, LookupIdMap.class);
        Assert.assertNotNull(lookupIdConfigs);
        Assert.assertTrue(lookupIdConfigs.keySet().size() > 0);

        lookupIdConfigs.keySet().stream().forEach(k -> {
            CDLExternalSystemType externalSystemType = CDLExternalSystemType.valueOf(k);
            Assert.assertTrue(lookupIdConfigs.get(k).size() > 0);
            lookupIdConfigs.get(k).stream().forEach(c -> {
                Assert.assertNotNull(c);
                Assert.assertEquals(c.getExternalSystemType(), externalSystemType);
                Assert.assertNotNull(c.getId());
                Assert.assertNotNull(c.getOrgId());
                Assert.assertNotNull(c.getOrgName());

                LookupIdMap lookupIdMap = restTemplate.getForObject(
                        getRestAPIHostPort() + "/pls/lookup-id-mapping/config/" + c.getId(), LookupIdMap.class);
                Assert.assertNotNull(lookupIdMap);
                Assert.assertEquals(lookupIdMap.getExternalSystemType(), externalSystemType);
                Assert.assertNotNull(lookupIdMap.getId());
                Assert.assertNotNull(lookupIdMap.getOrgId());
                Assert.assertNotNull(lookupIdMap.getOrgName());
                Assert.assertEquals(lookupIdMap.getId(), c.getId());
                Assert.assertNotNull(lookupIdMap.getIsRegistered());
                Assert.assertEquals(lookupIdMap.getIsRegistered(), isMarkedRegistered);
            });
        });
    }
}
