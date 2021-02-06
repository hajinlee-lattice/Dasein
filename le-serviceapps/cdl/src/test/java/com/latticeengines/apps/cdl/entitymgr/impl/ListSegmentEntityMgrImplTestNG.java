package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentConfig;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ListSegmentEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ListSegmentEntityMgrImplTestNG.class);

    private static final String commonResourcePath = "metadata/";
    private final String listSegmentCSVAdaptorPath = "ListSegmentCSVAdaptor.json";

    private String listSegmentName;

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private ListSegmentEntityMgr listSegmentEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        listSegmentName = NamingUtils.uuid("LIST_SEGMENT_NAME");
        setupTestEnvironmentWithDataCollection();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "functional")
    public void createOrUpdateListSegment() {
        MetadataSegment metadataSegment = new MetadataSegment();
        String segmentDisplayName = "list-segment-display-name";
        String segmentDescription = "list-segment-description";
        String externalSystem = "dataVision";
        String externalSegmentId = "dataVisionSegment";
        String s3DropFolder = "/latticeengines-qa-data-stage/datavision_segment/" + listSegmentName + "/input";
        ListSegment listSegment = createListSegment(externalSystem, externalSegmentId, s3DropFolder);
        metadataSegment.setName(listSegmentName);
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);
        metadataSegment.setDataCollection(dataCollection);
        metadataSegment.setType(MetadataSegment.SegmentType.List);
        metadataSegment.setTenant(MultiTenantContext.getTenant());
        metadataSegment.setListSegment(listSegment);
        segmentEntityMgr.createListSegment(metadataSegment);

        listSegment = listSegmentEntityMgr.findByExternalInfo(listSegment.getExternalSystem(), listSegment.getExternalSegmentId());
        assertNotNull(listSegment);
        listSegment.setCsvAdaptor(generateCSVAdaptor());
        listSegment.setDataTemplates(getDataTemplates());
        listSegmentEntityMgr.updateListSegment(listSegment);
        listSegment = listSegmentEntityMgr.findByExternalInfo(listSegment.getExternalSystem(), listSegment.getExternalSegmentId());
        verifyListSegment(listSegment);
        assertEquals(listSegment.getDataTemplates().size(), 2);
    }

    private void verifyListSegment(ListSegment listSegment) {
        assertNotNull(listSegment);
        CSVAdaptor csvAdaptor = listSegment.getCsvAdaptor();
        assertNotNull(csvAdaptor);
        assertNotNull(csvAdaptor.getImportFieldMappings());
        assertEquals(csvAdaptor.getImportFieldMappings().size(), 27);
        ImportFieldMapping importFieldMapping = csvAdaptor.getImportFieldMappings().get(0);
        assertEquals(importFieldMapping.getFieldName(), InterfaceName.CompanyName.name());
        assertEquals(importFieldMapping.getFieldType(), UserDefinedType.TEXT);
        assertEquals(importFieldMapping.getUserFieldName(), "Company Name");
        assertNotNull(listSegment.getConfig());
        assertTrue(listSegment.getConfig().isNeedToMatch());
        Map<String, String> displayNames = listSegment.getConfig().getDisplayNames();
        assertNotNull(displayNames);
        assertEquals(displayNames.get("attr1"), "displayName1");
        assertEquals(displayNames.get("attr2"), "displayName2");
    }

    private Map<String, String> getDataTemplates() {
        Map<String, String> dataTemplates = new HashMap<>();
        dataTemplates.put(BusinessEntity.Account.name(), UUID.randomUUID().toString());
        dataTemplates.put(BusinessEntity.Contact.name(), UUID.randomUUID().toString());
        return dataTemplates;
    }

    private CSVAdaptor generateCSVAdaptor() {
        CSVAdaptor csvAdaptor = JsonUtils.deserialize(
                getStaticDocument(commonResourcePath + listSegmentCSVAdaptorPath), CSVAdaptor.class);
        return csvAdaptor;
    }

    private ListSegment createListSegment(String externalSystem, String externalSegmentId, String s3DropFolder) {
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(externalSystem);
        listSegment.setExternalSegmentId(externalSegmentId);
        listSegment.setS3DropFolder(s3DropFolder);
        ListSegmentConfig listSegmentConfig = new ListSegmentConfig();
        listSegmentConfig.setNeedToMatch(true);
        Map<String, String> displayNames = new HashMap<>();
        displayNames.put("attr1", "displayName1");
        displayNames.put("attr2", "displayName2");
        listSegmentConfig.setDisplayNames(displayNames);
        listSegment.setConfig(listSegmentConfig);
        return listSegment;
    }

    private InputStream getStaticDocument(String documentPath) {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            return classLoader.getResourceAsStream(documentPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_10011, e,
                    new String[]{documentPath.replace(commonResourcePath, "")});
        }
    }
}
