package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ExportFieldMetadataServiceImplUnitTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ExportFieldMetadataDefaultsService exportService;

    private List<ExportFieldMetadataDefaults> defaultMarketoExportFields;
    private List<ExportFieldMetadataDefaults> defaultS3ExportFields;
    private List<ExportFieldMetadataDefaults> defaultLinkedInExportFields;
    private List<ExportFieldMetadataDefaults> defaultFacebookExportFields;
    private List<ExportFieldMetadataDefaults> defaultOutreachExportFields;
    private List<ExportFieldMetadataDefaults> defaultGoogleAdsExportFields;
    private List<ExportFieldMetadataDefaults> defaultAdobeAudienceMgrExportFields;
    private List<ExportFieldMetadataDefaults> defaultAppNexusExportFields;
    private List<ExportFieldMetadataDefaults> defaultGoogleDisplayNVideo360ExportFields;
    private List<ExportFieldMetadataDefaults> defaultMediaMathExportFields;
    private List<ExportFieldMetadataDefaults> defaultTradeDeskExportFields;
    private List<ExportFieldMetadataDefaults> defaultVerizonMediaExportFields;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        defaultMarketoExportFields = getDefaultExportFields(CDLExternalSystemName.Marketo);

        defaultS3ExportFields = getDefaultExportFields(CDLExternalSystemName.AWS_S3);

        defaultLinkedInExportFields = getDefaultExportFields(CDLExternalSystemName.LinkedIn);

        defaultFacebookExportFields = getDefaultExportFields(CDLExternalSystemName.Facebook);

        defaultOutreachExportFields = getDefaultExportFields(CDLExternalSystemName.Outreach);

        defaultGoogleAdsExportFields = getDefaultExportFields(CDLExternalSystemName.GoogleAds);
        
        defaultAdobeAudienceMgrExportFields = getDefaultExportFields(CDLExternalSystemName.Adobe_Audience_Mgr);
        
        defaultAppNexusExportFields = getDefaultExportFields(CDLExternalSystemName.AppNexus);
        
        defaultGoogleDisplayNVideo360ExportFields = getDefaultExportFields(
                CDLExternalSystemName.Google_Display_N_Video_360);
        
        defaultMediaMathExportFields = getDefaultExportFields(CDLExternalSystemName.MediaMath);
        
        defaultTradeDeskExportFields = getDefaultExportFields(CDLExternalSystemName.TradeDesk);
        
        defaultVerizonMediaExportFields = getDefaultExportFields(CDLExternalSystemName.Verizon_Media);

    }

    @AfterClass(groups = "functional")
    public void teardown() {

    }

    @Test(groups = "functional")
    public void testMarketo() {
        defaultMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);

        assertEquals(defaultMarketoExportFields.size(), 41);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                25);

    }

    @Test(groups = "functional")
    public void testUpdateFieldsMarketo() {
        List<ExportFieldMetadataDefaults> originalMarketoExportFields = exportService
                .getAllAttributes(CDLExternalSystemName.Marketo);
        long count1 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField)
                .count();
        assertEquals(count1, 16);
        ExportFieldMetadataDefaults firstField = originalMarketoExportFields.get(0);
        firstField.setStandardField(!firstField.getStandardField());
        List<ExportFieldMetadataDefaults> toSave = new ArrayList<>();
        toSave.add(firstField);
        exportService.updateDefaultFields(CDLExternalSystemName.Marketo, toSave);

        originalMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);
        long count2 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField)
                .count();
        assertEquals(count2, 17);

        firstField.setStandardField(!firstField.getStandardField());
        toSave = new ArrayList<>();
        toSave.add(firstField);
        toSave.add(firstField);
        exportService.updateDefaultFields(CDLExternalSystemName.Marketo, toSave);
        originalMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);
        long count3 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField)
                .count();
        assertEquals(count3, 16);
    }

    @Test(groups = "functional")
    public void testS3() {
        defaultS3ExportFields = exportService.getAllAttributes(CDLExternalSystemName.AWS_S3);

        assertEquals(defaultS3ExportFields.size(), 45);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(), 37);

        List<ExportFieldMetadataDefaults> enabledS3ExportFields = exportService
                .getExportEnabledAttributes(CDLExternalSystemName.AWS_S3);
        List<ExportFieldMetadataDefaults> enabledAccountS3ExportFields = exportService
                .getExportEnabledAttributesForEntity(CDLExternalSystemName.AWS_S3, BusinessEntity.Account);
        List<ExportFieldMetadataDefaults> enabledContactS3ExportFields = exportService
                .getExportEnabledAttributesForEntity(CDLExternalSystemName.AWS_S3, BusinessEntity.Contact);
        assertEquals(enabledS3ExportFields.size(), 37);
        assertEquals(enabledAccountS3ExportFields.size(), 21);
        assertEquals(enabledContactS3ExportFields.size(), 16);
    }

    @Test(groups = "functional")
    public void testLinkedIn() {
        defaultLinkedInExportFields = exportService.getAllAttributes(CDLExternalSystemName.LinkedIn);

        assertEquals(defaultLinkedInExportFields.size(), 55);
        List<ExportFieldMetadataDefaults> exportEnabledFields = defaultLinkedInExportFields.stream()
                .filter(ExportFieldMetadataDefaults::getExportEnabled).collect((Collectors.toList()));
        assertEquals(exportEnabledFields.size(), 25);

        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Account).count(),
                14);
        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Contact).count(),
                11);
    }

    //
    @Test(groups = "functional")
    public void testFacebook() {
        defaultFacebookExportFields = exportService.getAllAttributes(CDLExternalSystemName.Facebook);

        assertEquals(defaultFacebookExportFields.size(), 47);
        assertEquals(defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                11);

    }

    @Test(groups = "functional")
    public void testGoogleAds() {
        defaultGoogleAdsExportFields = exportService.getAllAttributes(CDLExternalSystemName.GoogleAds);

        assertEquals(defaultGoogleAdsExportFields.size(), 43);
        assertEquals(defaultGoogleAdsExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                10);

    }

    @Test(groups = "functional")
    public void testOutreach() {
        defaultOutreachExportFields = exportService.getAllAttributes(CDLExternalSystemName.Outreach);

        assertEquals(defaultOutreachExportFields.size(), 37);
        assertEquals(defaultOutreachExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                27);

    }

    @Test(groups = "functional")
    public void testAdobeAudienceMgr() {
        defaultAdobeAudienceMgrExportFields = exportService.getAllAttributes(CDLExternalSystemName.Adobe_Audience_Mgr);

        assertEquals(defaultAdobeAudienceMgrExportFields.size(), 1);
        assertEquals(defaultAdobeAudienceMgrExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled)
                .count(),
                1);

    }

    @Test(groups = "functional")
    public void testAppNexus() {
        defaultAppNexusExportFields = exportService.getAllAttributes(CDLExternalSystemName.AppNexus);

        assertEquals(defaultAppNexusExportFields.size(), 1);
        assertEquals(defaultAppNexusExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                1);

    }

    @Test(groups = "functional")
    public void testGoogleDisplayNVideo360() {
        defaultGoogleDisplayNVideo360ExportFields = exportService
                .getAllAttributes(CDLExternalSystemName.Google_Display_N_Video_360);

        assertEquals(defaultGoogleDisplayNVideo360ExportFields.size(), 1);
        assertEquals(defaultGoogleDisplayNVideo360ExportFields.stream()
                .filter(ExportFieldMetadataDefaults::getExportEnabled).count(), 1);

    }

    @Test(groups = "functional")
    public void testMediaMath() {
        defaultMediaMathExportFields = exportService.getAllAttributes(CDLExternalSystemName.MediaMath);

        assertEquals(defaultMediaMathExportFields.size(), 1);
        assertEquals(
                defaultMediaMathExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(), 1);

    }

    @Test(groups = "functional")
    public void testTradeDesk() {
        defaultTradeDeskExportFields = exportService.getAllAttributes(CDLExternalSystemName.TradeDesk);

        assertEquals(defaultTradeDeskExportFields.size(), 1);
        assertEquals(
                defaultTradeDeskExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(), 1);

    }

    @Test(groups = "functional")
    public void testVerizonMedia() {
        defaultVerizonMediaExportFields = exportService.getAllAttributes(CDLExternalSystemName.Verizon_Media);

        assertEquals(defaultVerizonMediaExportFields.size(), 1);
        assertEquals(defaultVerizonMediaExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                1);

    }

    private List<ExportFieldMetadataDefaults> getDefaultExportFields(CDLExternalSystemName systemName) {
        List<ExportFieldMetadataDefaults> defaultExportFields = exportService.getAllAttributes(systemName);

        if (defaultExportFields.size() == 0) {
            createDefaultExportFields(systemName);
        } else {
            defaultExportFields = updateFieldMetadataDefault(systemName);
        }

        return defaultExportFields;
    }

    private List<ExportFieldMetadataDefaults> createDefaultExportFields(CDLExternalSystemName systemName) {
        List<ExportFieldMetadataDefaults> defaultExportFields = getExportFieldMetadataDefaultsFromJson(systemName);
        exportService.createDefaultExportFields(defaultExportFields);
        return defaultExportFields;
    }

    private List<ExportFieldMetadataDefaults> updateFieldMetadataDefault(CDLExternalSystemName systemName) {
        List<ExportFieldMetadataDefaults> defaultExportFields = getExportFieldMetadataDefaultsFromJson(systemName);
        return exportService.updateDefaultFields(systemName, defaultExportFields);
    }

    private List<ExportFieldMetadataDefaults> getExportFieldMetadataDefaultsFromJson(CDLExternalSystemName systemName) {
        String filePath = String.format("service/impl/%s_default_export_fields.json",
                systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
                .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);
        return defaultExportFields;
    }
}
