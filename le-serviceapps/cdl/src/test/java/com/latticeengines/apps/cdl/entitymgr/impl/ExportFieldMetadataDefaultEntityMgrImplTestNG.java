package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.fail;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataDefaultsEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ExportFieldMetadataDefaultEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ExportFieldMetadataDefaultsEntityMgr defaultExportFieldMetadataEntityMgr;

    private List<CDLExternalSystemName> systemsToTest = Arrays.asList(
            CDLExternalSystemName.Marketo,
            CDLExternalSystemName.AWS_S3,
            CDLExternalSystemName.LinkedIn,
            CDLExternalSystemName.Facebook,
            CDLExternalSystemName.Outreach, CDLExternalSystemName.GoogleAds);

    private Map<CDLExternalSystemName, List<ExportFieldMetadataDefaults>> defaultExportFieldsFromJsonMap;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        defaultExportFieldsFromJsonMap = new HashMap<>();

        for (CDLExternalSystemName systemName : systemsToTest) {
            loadDefaultExportFieldsIfMissing(systemName);

            defaultExportFieldsFromJsonMap.put(systemName, getDefaultExportFieldsFromJson(systemName));
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() {

    }

    @Test(groups = "functional")
    public void testGetAllDefaultExportFieldMetadata() {
        for (CDLExternalSystemName systemName : systemsToTest) {
            List<ExportFieldMetadataDefaults> defaultExportFieldsFromJson = defaultExportFieldsFromJsonMap
                    .get(systemName);

            List<ExportFieldMetadataDefaults> defaultExportFields = defaultExportFieldMetadataEntityMgr
                    .getAllDefaultExportFieldMetadata(systemName);

            if (!defaultExportFieldListsEqual(defaultExportFields, defaultExportFieldsFromJson)) {
                String failMsg = String.format(
                        "%s: Database does not match JSON for all fields. Database length: %d JSON length: %d",
                        systemName.toString(), defaultExportFields.size(), defaultExportFieldsFromJson.size());
                fail(failMsg);
            }
        }
    }

    @Test(groups = "functional")
    public void testGetExportEnabledDefaultFieldMetadata() {
        for (CDLExternalSystemName systemName : systemsToTest) {
            List<ExportFieldMetadataDefaults> defaultExportFieldsFromJson = defaultExportFieldsFromJsonMap
                    .get(systemName);

            List<ExportFieldMetadataDefaults> exportEnabledDefaultFields = defaultExportFieldMetadataEntityMgr
                    .getExportEnabledDefaultFieldMetadata(systemName);

            List<ExportFieldMetadataDefaults> exportEnabledDefaultFieldsFromJson = defaultExportFieldsFromJson.stream()
                    .filter(field -> field.getExternalSystemName().equals(systemName)
                            && field.getExportEnabled() == true)
                    .collect(Collectors.toList());

            if (!defaultExportFieldListsEqual(exportEnabledDefaultFields, exportEnabledDefaultFieldsFromJson)) {
                String failMsg = String.format(
                        "%s: Database does not match JSON for export enabled. Database length: %d JSON length: %d",
                        systemName.toString(), exportEnabledDefaultFields.size(),
                        exportEnabledDefaultFieldsFromJson.size());
                fail(failMsg);
            }
        }
    }

    @Test(groups = "functional")
    public void testGetExportEnabledDefaultFieldMetadataForEntity() {
        for (CDLExternalSystemName systemName : systemsToTest) {
            List<ExportFieldMetadataDefaults> defaultExportFieldsFromJson = defaultExportFieldsFromJsonMap
                    .get(systemName);

            for (BusinessEntity businessEntity : BusinessEntity.values()) {
                List<ExportFieldMetadataDefaults> defaultExportFieldsForEntity = defaultExportFieldMetadataEntityMgr
                        .getExportEnabledDefaultFieldMetadataForEntity(systemName, businessEntity);

                List<ExportFieldMetadataDefaults> defaultExportFieldsForEntityFromJson = defaultExportFieldsFromJson
                        .stream()
                        .filter(field -> field.getExternalSystemName().equals(systemName)
                                && field.getExportEnabled() == true && field.getEntity().equals(businessEntity))
                        .collect(Collectors.toList());

                if (!defaultExportFieldListsEqual(defaultExportFieldsForEntity, defaultExportFieldsForEntityFromJson)) {
                    String failMsg = String.format(
                            "%s: Database does not match JSON for entity. Database length: %d JSON length: %d",
                            systemName.toString(), defaultExportFieldsForEntity.size(),
                            defaultExportFieldsForEntityFromJson.size());
                    fail(failMsg);
                }
            }
        }
    }

    @Test(groups = "functional")
    public void testGetExportEnabledDefaultFieldMetadataForAudienceType() {
        List<CDLExternalSystemName> systemsToTest = Arrays.asList(CDLExternalSystemName.AWS_S3,
                CDLExternalSystemName.LinkedIn, CDLExternalSystemName.Facebook, CDLExternalSystemName.GoogleAds);

        for (CDLExternalSystemName systemName : systemsToTest) {
            List<ExportFieldMetadataDefaults> defaultExportFieldsFromJson = defaultExportFieldsFromJsonMap
                    .get(systemName);

            for (AudienceType audienceType : AudienceType.values()) {
                List<ExportFieldMetadataDefaults> defaultExportFieldsForAudienceType = defaultExportFieldMetadataEntityMgr
                        .getExportEnabledDefaultFieldMetadataForAudienceType(systemName, audienceType);

                List<ExportFieldMetadataDefaults> defaultExportFieldsForAudienceTypeFromJson = defaultExportFieldsFromJson
                        .stream()
                        .filter(field -> field.getExternalSystemName().equals(systemName)
                                && field.getExportEnabled() == true && field.getAudienceTypes().contains(audienceType))
                        .collect(Collectors.toList());

                if (!defaultExportFieldListsEqual(defaultExportFieldsForAudienceType,
                        defaultExportFieldsForAudienceTypeFromJson)) {
                    String failMsg = String.format(
                            "%s: Database does not match JSON for audience type. Database length: %d JSON length: %d",
                            systemName.toString(), defaultExportFieldsForAudienceType.size(),
                            defaultExportFieldsForAudienceTypeFromJson.size());
                    fail(failMsg);
                }
            }
        }
    }

    @Test(groups = "functional")
    public void testGetHistoryEnabledDefaultFieldMetadata() {
        for (CDLExternalSystemName systemName : systemsToTest) {
            List<ExportFieldMetadataDefaults> defaultExportFieldsFromJson = defaultExportFieldsFromJsonMap
                    .get(systemName);

            List<ExportFieldMetadataDefaults> historyEnabledDefaultFields = defaultExportFieldMetadataEntityMgr
                    .getHistoryEnabledDefaultFieldMetadata(systemName);

            List<ExportFieldMetadataDefaults> historyEnabledDefaultFieldsFromJson = defaultExportFieldsFromJson.stream()
                    .filter(field -> field.getExternalSystemName().equals(systemName)
                            && field.getHistoryEnabled() == true)
                    .collect(Collectors.toList());

            if (!defaultExportFieldListsEqual(historyEnabledDefaultFields, historyEnabledDefaultFieldsFromJson)) {
                String failMsg = String.format(
                        "%s: Database does not match JSON for history enabled. Database length: %d JSON length: %d",
                        systemName.toString(), historyEnabledDefaultFields.size(),
                        historyEnabledDefaultFieldsFromJson.size());
                fail(failMsg);
            }
        }
    }

    private void loadDefaultExportFieldsIfMissing(CDLExternalSystemName systemName) {
        List<ExportFieldMetadataDefaults> defaultExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(systemName);
        
        if (defaultExportFields.size() == 0) {
            createDefaultExportFields(systemName);
        }
    }
    
    private List<ExportFieldMetadataDefaults> createDefaultExportFields(CDLExternalSystemName systemName) {
        List<ExportFieldMetadataDefaults> defaultExportFields = getDefaultExportFieldsFromJson(systemName);

        defaultExportFieldMetadataEntityMgr.createAll(defaultExportFields);
        return defaultExportFields;
    }

    private List<ExportFieldMetadataDefaults> getDefaultExportFieldsFromJson(CDLExternalSystemName systemName) {
        String filePath = String.format("service/impl/%s_default_export_fields.json",
                systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);

        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
                .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);

        return defaultExportFields;
    }

    private boolean defaultExportFieldListsEqual(
            List<ExportFieldMetadataDefaults> list1,
            List<ExportFieldMetadataDefaults> list2) {
        if (list1.size() != list2.size())
            return false;

        HashSet<String> list1InternalNames = new HashSet<>();
        for (ExportFieldMetadataDefaults fieldMetadata : list1) {
            list1InternalNames.add(fieldMetadata.getAttrName());
        }
        for (ExportFieldMetadataDefaults fieldMetadata : list2) {
            if (!list1InternalNames.contains(fieldMetadata.getAttrName()))
                return false;
        }
        return true;
    }

}
