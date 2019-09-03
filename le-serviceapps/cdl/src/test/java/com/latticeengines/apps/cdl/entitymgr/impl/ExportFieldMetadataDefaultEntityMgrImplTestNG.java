package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataDefaultsEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ExportFieldMetadataDefaultEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private ExportFieldMetadataDefaultsEntityMgr defaultExportFieldMetadataEntityMgr;

    List<ExportFieldMetadataDefaults> defaultMarketoExportFields;
    List<ExportFieldMetadataDefaults> defaultS3ExportFields;
    List<ExportFieldMetadataDefaults> defaultLinkedInExportFields;
    List<ExportFieldMetadataDefaults> defaultFacebookExportFields;
    List<ExportFieldMetadataDefaults> defaultOutreachExportFields;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        defaultMarketoExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Marketo);

        if (defaultMarketoExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.Marketo);
        }else {
            defaultMarketoExportFields = updateFieldMetadataDefault(CDLExternalSystemName.Marketo, defaultMarketoExportFields);
        }

        defaultS3ExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.AWS_S3);

        if (defaultS3ExportFields.size() == 0) {
            createDefaultExportFields(CDLExternalSystemName.AWS_S3);
        }else {
            defaultS3ExportFields = updateFieldMetadataDefault(CDLExternalSystemName.AWS_S3, defaultS3ExportFields);
        }

        defaultLinkedInExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.LinkedIn);

        if (defaultLinkedInExportFields.size() == 0) {
            defaultLinkedInExportFields = createDefaultExportFields(CDLExternalSystemName.LinkedIn);
        }else {
            defaultLinkedInExportFields = updateFieldMetadataDefault(CDLExternalSystemName.LinkedIn, defaultLinkedInExportFields);
        }

        defaultFacebookExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Facebook);

        if (defaultFacebookExportFields.size() == 0) {
            defaultFacebookExportFields = createDefaultExportFields(CDLExternalSystemName.Facebook);
        }else {
            defaultFacebookExportFields = updateFieldMetadataDefault(CDLExternalSystemName.Facebook, defaultFacebookExportFields);
        }


        defaultOutreachExportFields = defaultExportFieldMetadataEntityMgr
            .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Outreach);

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
        defaultMarketoExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Marketo);

        assertEquals(defaultMarketoExportFields.size(), 41);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
                34);
        assertEquals(defaultMarketoExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                24);


    }

    @Test(groups = "functional")
    public void testS3() {
        defaultS3ExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.AWS_S3);

        assertEquals(defaultS3ExportFields.size(), 41);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(), 34);
        assertEquals(defaultS3ExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                41);

    }

    @Test(groups = "functional")
    public void testLinkedIn() {
        defaultLinkedInExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.LinkedIn);

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

    @Test(groups = "functional")
    public void testFacebook() {
        defaultFacebookExportFields = defaultExportFieldMetadataEntityMgr
                .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Facebook);

        assertEquals(defaultFacebookExportFields.size(), 41);
        assertEquals(
                defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getHistoryEnabled).count(),
                34);
        assertEquals(defaultFacebookExportFields.stream().filter(ExportFieldMetadataDefaults::getExportEnabled).count(),
                11);

    }

    @Test(groups = "functional")
    public void testOutreach() {
        defaultOutreachExportFields = defaultExportFieldMetadataEntityMgr
            .getAllDefaultExportFieldMetadata(CDLExternalSystemName.Outreach);

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
        defaultExportFieldMetadataEntityMgr.createAll(defaultExportFields);
        return defaultExportFields;
    }

    private List<ExportFieldMetadataDefaults> updateFieldMetadataDefault(CDLExternalSystemName systemName, List<ExportFieldMetadataDefaults> oldDefaultExportFields){
        String filePath = String.format("service/impl/%s_default_export_fields.json",
            systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
            .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);

        List<ExportFieldMetadataDefaults> listToSave = new ArrayList<>();
        List<ExportFieldMetadataDefaults> listToCreate = new ArrayList<>();
        defaultExportFields.forEach( defaultField -> {
            ExportFieldMetadataDefaults updated = oldDefaultExportFields.stream()
                .filter( oldField -> defaultField.getAttrName().equals(oldField.getAttrName()) && defaultField.getExternalSystemName().equals((oldField.getExternalSystemName())))
                .findAny()
                .orElse(null);
            if(updated != null){
                defaultField.setPid(updated.getPid());
                log.info(defaultField.getAttrName() + "  " + defaultField.getStandardField());
                listToSave.add(defaultField);
            }else {
                listToCreate.add(defaultField);
            }
        });
        List<ExportFieldMetadataDefaults> listCreated = addNewFields(systemName, listToCreate);
        List<ExportFieldMetadataDefaults> finalList = Stream.of(listCreated, listToSave)
            .flatMap(x -> x.stream())
            .collect(Collectors.toList());
        return defaultExportFieldMetadataEntityMgr.updateDefaultFields(systemName, finalList);
    }

    private List<ExportFieldMetadataDefaults> addNewFields(CDLExternalSystemName systemName, List<ExportFieldMetadataDefaults> newFields){
        if(!newFields.isEmpty()) {
            return defaultExportFieldMetadataEntityMgr.createAll(newFields);
        } else {
            return newFields;
        }
    }


}
