package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.List;
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

    List<ExportFieldMetadataDefaults> defaultMarketoExportFields;
    List<ExportFieldMetadataDefaults> defaultS3ExportFields;
    List<ExportFieldMetadataDefaults> defaultLinkedInExportFields;
    List<ExportFieldMetadataDefaults> defaultFacebookExportFields;
    List<ExportFieldMetadataDefaults> defaultOutreachExportFields;
    List<ExportFieldMetadataDefaults> defaultGoogleAdsExportFields;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        defaultMarketoExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Marketo);

        if (defaultMarketoExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.Marketo);
        }

        defaultS3ExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.AWS_S3);

        if (defaultS3ExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.AWS_S3);
        }

        defaultLinkedInExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.LinkedIn);

        if (defaultLinkedInExportFields.size() == 0) {
            defaultLinkedInExportFields = createDefaultExportFields(CDLExternalSystemName.LinkedIn);
        }

        defaultFacebookExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Facebook);

        if (defaultFacebookExportFields.size() == 0) {
            defaultFacebookExportFields = createDefaultExportFields(CDLExternalSystemName.Facebook);
        }

        defaultOutreachExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Outreach);

        if (defaultOutreachExportFields.size() == 0) {
            defaultOutreachExportFields = createDefaultExportFields(CDLExternalSystemName.Outreach);
        }

        defaultGoogleAdsExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.GoogleAds);

        if (defaultGoogleAdsExportFields.size() == 0) {
            defaultGoogleAdsExportFields = createDefaultExportFields(CDLExternalSystemName.GoogleAds);
        }

    }

    @AfterClass(groups = "functional")
    public void teardown() {

    }

    @Test(groups = "functional")
    public void testMarketo() {
        defaultMarketoExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Marketo);

        assertEquals(defaultMarketoExportFields.size(), 41);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
                34);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                23);

    }

    @Test(groups = "functional")
    public void testS3() {
        defaultS3ExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.AWS_S3);

        assertEquals(defaultS3ExportFields.size(), 41);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(), 34);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(), 32);

    }

    @Test(groups = "functional")
    public void testLinkedIn() {
        defaultLinkedInExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.LinkedIn);

        assertEquals(defaultLinkedInExportFields.size(), 55);
        List<ExportFieldMetadataDefaults> exportEnabledFields = defaultLinkedInExportFields.stream()
                .filter(ExportFieldMetadataDefaults::getExportEnabled).collect((Collectors.toList()));
        assertEquals(defaultLinkedInExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                25);

        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Account).count(),
                14);
        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Contact).count(),
                11);

    }

    @Test(groups = "functional")
    public void testLinkedInAccounts() {
        List<ExportFieldMetadataDefaults> defaultLinkedInAccountExportFields = defaultExportFieldMetadataEntityMgr
                .getExportEnabledDefaultFieldMetadataForAudienceType(CDLExternalSystemName.LinkedIn,
                        AudienceType.ACCOUNTS);

        assertEquals(defaultLinkedInAccountExportFields.size(), 14);
    }

    @Test(groups = "functional")
    public void testLinkedInContacts() {
        List<ExportFieldMetadataDefaults> defaultLinkedInContactsExportFields = defaultExportFieldMetadataEntityMgr
                .getExportEnabledDefaultFieldMetadataForAudienceType(CDLExternalSystemName.LinkedIn,
                        AudienceType.CONTACTS);

        assertEquals(defaultLinkedInContactsExportFields.size(), 13);
    }

    @Test(groups = "functional")
    public void testFacebook() {
        defaultFacebookExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Facebook);

        assertEquals(defaultFacebookExportFields.size(), 47);
        assertEquals(
                defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
                40);
        assertEquals(defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                10);

    }
    
    @Test(groups = "functional")
    public void testFacebookContacts() {
        List<ExportFieldMetadataDefaults> defaultFacebookContactsExportFields = defaultExportFieldMetadataEntityMgr
                .getExportEnabledDefaultFieldMetadataForAudienceType(CDLExternalSystemName.Facebook,
                        AudienceType.CONTACTS);

        assertEquals(defaultFacebookContactsExportFields.size(), 10);
    }

    @Test(groups = "functional")
    public void testOutreach() {
        defaultOutreachExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Outreach);

        assertEquals(defaultOutreachExportFields.size(), 37);
        assertEquals(
                defaultOutreachExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
                27);
        assertEquals(defaultOutreachExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                27);

    }

    @Test(groups = "functional")
    public void testGoogleAds() {
        defaultGoogleAdsExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.GoogleAds);

        assertEquals(defaultGoogleAdsExportFields.size(), 43);
        assertEquals(
                defaultGoogleAdsExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                10);

    }

    @Test(groups = "functional")
    public void testGoogleAdsContacts() {
        List<ExportFieldMetadataDefaults> defaultGoogleAdsExportFields = defaultExportFieldMetadataEntityMgr
                .getExportEnabledDefaultFieldMetadataForAudienceType(CDLExternalSystemName.GoogleAds,
                        AudienceType.CONTACTS);

        assertEquals(defaultGoogleAdsExportFields.size(), 10);
    }

    private List<ExportFieldMetadataDefaults> createDefaultExportFields(CDLExternalSystemName systemName) {
        String filePath = String.format("service/impl/%s_default_export_fields.json",
                systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);


        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
                .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);

        defaultExportFieldMetadataEntityMgr.createAll(defaultExportFields);
        return defaultExportFields;
    }

}
