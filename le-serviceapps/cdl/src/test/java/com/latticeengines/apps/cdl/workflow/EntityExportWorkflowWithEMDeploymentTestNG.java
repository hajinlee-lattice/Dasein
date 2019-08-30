package com.latticeengines.apps.cdl.workflow;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.end2end.CDLEnd2EndDeploymentTestNGBase;
import com.latticeengines.apps.cdl.end2end.ProcessAccountWithAdvancedMatchDeploymentTestNG;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

/**
 * dpltc deploy -a admin,pls,lp,cdl,metadata,matchapi,workflowapi
 */
public class EntityExportWorkflowWithEMDeploymentTestNG extends EntityExportWorkflowDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(EntityExportWorkflowWithEMDeploymentTestNG.class);

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH_GA enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        checkpointService.resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT, CDLEnd2EndDeploymentTestNGBase.S3_CHECKPOINTS_VERSION);
        log.info("Setup Complete!");
        configExportAttrs();
        saveCsvToLocal = false;
    }

    private void configExportAttrs() {
        AttrConfig enable1 = enableExport(BusinessEntity.Account, "user_Test_Date"); // date attr
        AttrConfig enable2 = enableExport(BusinessEntity.Account, "TechIndicator_OracleCommerce"); // bit encode
        AttrConfig enable3 = enableExport(BusinessEntity.Account, "CHIEF_EXECUTIVE_OFFICER_NAME"); // non-segmentable
        AttrConfig enable4 = enableExport(BusinessEntity.PurchaseHistory,
                "AM_g8cH04Lzvb0Mhou2lvuuSJjjvQm1KQ3J__W_1__SW"); // activity metric
        AttrConfigRequest request2 = new AttrConfigRequest();
        request2.setAttrConfigs(Arrays.asList(enable1, enable2, enable3, enable4));
        attrConfigService.saveRequest(request2, AttrConfigUpdateMode.Usage);
    }

    private AttrConfig enableExport(BusinessEntity entity, String attribute) {
        AttrConfig attrConfig = new AttrConfig();
        attrConfig.setEntity(entity);
        attrConfig.setAttrName(attribute);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(Boolean.TRUE);
        attrConfig.setAttrProps(new HashMap<>(ImmutableMap.of(ColumnSelection.Predefined.Enrichment.name(), enrichProp)));
        return attrConfig;
    }

    private void verifyAccountAndContactId(Map<BusinessEntity, List<ColumnMetadata>> schemaMap, Map<String, Integer> headerMap,
                                           BusinessEntity businessEntity, InterfaceName id) {
        List<ColumnMetadata> schema = schemaMap.getOrDefault(businessEntity, Collections.emptyList());
        String idDisplayName = "";
        for (ColumnMetadata columnMetadata : schema) {
            if (id.name().equals(columnMetadata.getAttrName())) {
                idDisplayName = columnMetadata.getDisplayName();
                break;
            }
        }
        Assert.assertTrue(StringUtils.isNotEmpty(idDisplayName));
        Assert.assertFalse(headerMap.containsKey(idDisplayName), "Header map: " + JsonUtils.serialize(headerMap));
    }

    protected void verifyCsvGzHeder(Map<String, Integer> headerMap) {
        // make sure no account and contact id
        Map<BusinessEntity, List<ColumnMetadata>> schemaMap = getExportSchema();
        verifyAccountAndContactId(schemaMap, headerMap, BusinessEntity.Account, InterfaceName.AccountId);
        verifyAccountAndContactId(schemaMap, headerMap, BusinessEntity.Contact, InterfaceName.ContactId);
        Assert.assertTrue(headerMap.containsKey("CEO Name"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Test Date"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey("Has Oracle Commerce"), "Header map: " + JsonUtils.serialize(headerMap));
        Assert.assertTrue(headerMap.containsKey(InterfaceName.AtlasExportTime.name()), "Header map: " + JsonUtils.serialize(headerMap));
    }

    private Map<BusinessEntity, List<ColumnMetadata>> getExportSchema() {
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        Map<BusinessEntity, List<ColumnMetadata>> schemaMap = new HashMap<>();
        for (BusinessEntity entity : BusinessEntity.EXPORT_ENTITIES) {
            List<ColumnMetadata> cms = servingStoreProxy //
                    .getDecoratedMetadata(mainTestCustomerSpace.toString(), entity, groups, version).collectList().block();
            if (CollectionUtils.isNotEmpty(cms)) {
                schemaMap.put(entity, cms);
            }
            log.info("Found " + CollectionUtils.size(cms) + " attrs to export for " + entity);
        }
        return schemaMap;
    }
}
