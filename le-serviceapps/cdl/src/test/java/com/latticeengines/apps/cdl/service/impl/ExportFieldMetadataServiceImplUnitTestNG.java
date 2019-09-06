package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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


    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private ExportFieldMetadataDefaultsService exportService;

    List<ExportFieldMetadataDefaults> defaultMarketoExportFields;
    List<ExportFieldMetadataDefaults> defaultS3ExportFields;
    List<ExportFieldMetadataDefaults> defaultLinkedInExportFields;
    List<ExportFieldMetadataDefaults> defaultFacebookExportFields;
    List<ExportFieldMetadataDefaults> defaultOutreachExportFields;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        defaultMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);

        if (defaultMarketoExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.Marketo);
        }else {
            defaultMarketoExportFields = updateFieldMetadataDefault(CDLExternalSystemName.Marketo, defaultMarketoExportFields);
        }

        defaultS3ExportFields = exportService.getAllAttributes(CDLExternalSystemName.AWS_S3);

        if (defaultS3ExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.AWS_S3);
        }else {
            defaultS3ExportFields = updateFieldMetadataDefault(CDLExternalSystemName.AWS_S3, defaultS3ExportFields);
        }

        defaultLinkedInExportFields = exportService.getAllAttributes(CDLExternalSystemName.LinkedIn);

        if (defaultLinkedInExportFields.size() == 0) {
            defaultLinkedInExportFields = createDefaultExportFields(CDLExternalSystemName.LinkedIn);
        }else {
            defaultLinkedInExportFields = updateFieldMetadataDefault(CDLExternalSystemName.LinkedIn, defaultLinkedInExportFields);
        }

        defaultFacebookExportFields = exportService.getAllAttributes(CDLExternalSystemName.Facebook);

        if (defaultFacebookExportFields.size() == 0) {
            defaultFacebookExportFields = createDefaultExportFields(CDLExternalSystemName.Facebook);
        }else {
            defaultFacebookExportFields = updateFieldMetadataDefault(CDLExternalSystemName.Facebook, defaultFacebookExportFields);
        }


        defaultOutreachExportFields = exportService.getAllAttributes(CDLExternalSystemName.Outreach);

        if (defaultOutreachExportFields.size() == 0) {
            defaultOutreachExportFields = createDefaultExportFields(CDLExternalSystemName.Outreach);
        }else {
            defaultOutreachExportFields = updateFieldMetadataDefault(CDLExternalSystemName.Outreach, defaultOutreachExportFields);
        }


    }

    @AfterClass(groups = "functional")
    public void teardown() {

    }

    @Test(groups = "functional")
    public void testMarketo() {
        defaultMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);

        assertEquals(defaultMarketoExportFields.size(), 41);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
            34);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
            24);


    }
    @Test(groups = "functional")
    public void testUpdateFieldsMarketo() {
        List<ExportFieldMetadataDefaults> originalMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);
        long count1 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField).count();
        assertEquals(count1, 14);
        ExportFieldMetadataDefaults firstField = originalMarketoExportFields.get(0);
        firstField.setStandardField(!firstField.getStandardField());
        List<ExportFieldMetadataDefaults> toSave = new ArrayList<>();
        toSave.add(firstField);
        exportService.updateDefaultFields(CDLExternalSystemName.Marketo, toSave);

        originalMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);
        long count2 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField).count();
        assertEquals(count2, 15);


        firstField.setStandardField(!firstField.getStandardField());
        toSave = new ArrayList<>();
        toSave.add(firstField);
        toSave.add(firstField);
        exportService.updateDefaultFields(CDLExternalSystemName.Marketo, toSave);
        originalMarketoExportFields = exportService.getAllAttributes(CDLExternalSystemName.Marketo);
        long count3 = originalMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getStandardField).count();
        assertEquals(count3, 14);
    }

    @Test(groups = "functional")
    public void testS3() {
        defaultS3ExportFields = exportService.getAllAttributes(CDLExternalSystemName.AWS_S3);

        assertEquals(defaultS3ExportFields.size(), 41);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(), 34);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
            41);

    }


    @Test(groups = "functional")
    public void testLinkedIn() {
        defaultLinkedInExportFields = exportService.getAllAttributes(CDLExternalSystemName.LinkedIn);

        assertEquals(defaultLinkedInExportFields.size(), 41);
        assertEquals(
            defaultLinkedInExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
            34);
        List<ExportFieldMetadataDefaults> exportEnabledFields = defaultLinkedInExportFields.stream()
            .filter(ExportFieldMetadataDefaults::getExportEnabled).collect((Collectors.toList()));
        assertEquals(defaultLinkedInExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
            3);

        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Account)
            .count(), 2);
        assertEquals(exportEnabledFields.stream().filter(field -> field.getEntity() == BusinessEntity.Contact)
            .count(), 1);

    }
//
    @Test(groups = "functional")
    public void testFacebook() {
        defaultFacebookExportFields = exportService.getAllAttributes(CDLExternalSystemName.Facebook);

        assertEquals(defaultFacebookExportFields.size(), 41);
        assertEquals(
            defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
            34);
        assertEquals(defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
            11);

    }

    @Test(groups = "functional")
    public void testOutreach() {
        defaultOutreachExportFields = exportService.getAllAttributes(CDLExternalSystemName.Outreach);

        assertEquals(defaultOutreachExportFields.size(), 42);
        assertEquals(
            defaultOutreachExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
            34);
        assertEquals(defaultOutreachExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
            27);

    }


    private List<ExportFieldMetadataDefaults> createDefaultExportFields(CDLExternalSystemName systemName) {
        String filePath = String.format("service/impl/%s_default_export_fields.json",
            systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
            .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);
        exportService.createDefaultExportFields(defaultExportFields);
        return defaultExportFields;
    }

    private List<ExportFieldMetadataDefaults> updateFieldMetadataDefault(CDLExternalSystemName systemName, List<ExportFieldMetadataDefaults> oldDefaultExportFields){
        String filePath = String.format("service/impl/%s_default_export_fields.json",
            systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
            .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);
        return exportService.updateDefaultFields(systemName, defaultExportFields);
    }


}
