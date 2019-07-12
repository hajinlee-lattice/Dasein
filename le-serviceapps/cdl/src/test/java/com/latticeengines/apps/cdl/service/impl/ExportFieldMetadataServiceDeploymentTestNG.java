package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataDefaultsService;
import com.latticeengines.apps.cdl.service.ExportFieldMetadataService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class ExportFieldMetadataServiceDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ExportFieldMetadataServiceDeploymentTestNG.class);

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private LookupIdMap lookupIdMap;

    private PlayLaunchChannel channel;

    private String org1 = "org1_" + CURRENT_TIME_MILLIS;
    private String org2 = "org1_" + CURRENT_TIME_MILLIS;
    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String PLAY_TARGET_SEGMENT_NAME = "Play Target Segment - 2";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private Date timestamp = new Date(System.currentTimeMillis());
    private Play play;
    private String PLAY_ID = "PLAY_ID";

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private ExportFieldMetadataDefaultsService exportFieldMetadataDefaultsService;

    @Inject
    private SegmentService segmentService;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateMetadata(mainTestTenant.getId(), 3);

        cleanupExportDefaults();

        MetadataSegment segment = constructSegment(PLAY_TARGET_SEGMENT_NAME);
        segment = segmentService.createOrUpdateSegment(segment);

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(mainTestTenant);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(segment);
        play.setPlayType(playTypeService.getAllPlayTypes(mainCustomerSpace).get(0));

        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);

        ArrayList<ExportFieldMetadataDefaults> defaultExportFields = new ArrayList<ExportFieldMetadataDefaults>();

        ExportFieldMetadataDefaults defaultField_1 = new ExportFieldMetadataDefaults();
        defaultField_1.setAttrName(InterfaceName.Email.toString());
        defaultField_1.setDisplayName(InterfaceName.Email.toString());
        defaultField_1.setEntity(BusinessEntity.Contact);
        defaultField_1.setExternalSystemName(CDLExternalSystemName.Marketo);
        defaultField_1.setStandardField(true);
        defaultField_1.setHistoryEnabled(true);
        defaultField_1.setExportEnabled(true);
        defaultField_1.setJavaClass("String");
        defaultExportFields.add(defaultField_1);
        
        ExportFieldMetadataDefaults defaultField_2 = new ExportFieldMetadataDefaults();
        defaultField_2.setAttrName(PLAY_ID);
        defaultField_2.setDisplayName("Campaign Id");
        defaultField_2.setEntity(BusinessEntity.Account);
        defaultField_2.setExternalSystemName(CDLExternalSystemName.AWS_S3);
        defaultField_2.setStandardField(false);
        defaultField_2.setHistoryEnabled(true);
        defaultField_2.setExportEnabled(true);
        defaultField_2.setJavaClass("String");
        defaultExportFields.add(defaultField_2);
        
        exportFieldMetadataDefaultsService.createDefaultExportFields(defaultExportFields);
    }

    @AfterClass(groups = { "deployment-app" })
    public void teardown() {
        cleanupExportDefaults();
    }

    private void cleanupExportDefaults() {
        List<ExportFieldMetadataDefaults> defaultFields = exportFieldMetadataDefaultsService
                .getAttributes(CDLExternalSystemName.AWS_S3);

        defaultFields.addAll(exportFieldMetadataDefaultsService
                .getAttributes(CDLExternalSystemName.Marketo));

        log.info(JsonUtils.serialize(defaultFields));
        exportFieldMetadataDefaultsService.delete(defaultFields);
    }

    @Test(groups = "deployment-app")
    public void testMarketoLaunch() {
        registerMarketoLookupIdMap();

        createPlayLaunchChannel(new MarketoChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 2);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        assertEquals(nonStandardFields.size(), 0);

    }

    @Test(groups = "deployment-app")
    public void testS3() {
        registerS3LookupIdMap();

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setIsIncludeExportAttributes(true);
        createPlayLaunchChannel(channelConfig, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 55);
        
        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        assertEquals(nonStandardFields.size(), 1);

    }

    private void createPlayLaunchChannel(ChannelConfig channelConfig, LookupIdMap lookupIdMap) {
        channel = new PlayLaunchChannel();
        channel.setTenant(mainTestTenant);
        channel.setCreatedBy(CREATED_BY);
        channel.setUpdatedBy(CREATED_BY);
        channel.setLaunchType(LaunchType.FULL);
        channel.setPlay(play);
        channel.setLaunchUnscored(true);

        channel.setChannelConfig(channelConfig);
        channel.setLookupIdMap(lookupIdMap);

        channel = playLaunchChannelService.create(channel);
    }

    private void registerMarketoLookupIdMap() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMap.setOrgId(org1);
        lookupIdMap.setOrgName("org1name");

        ExportFieldMetadataMapping fieldMapping_1 = new ExportFieldMetadataMapping();
        fieldMapping_1.setSourceField("CHIEF_EXECUTIVE_OFFICER_TITLE");
        fieldMapping_1.setDestinationField("ContactName");
        fieldMapping_1.setOverwriteValue(false);

        ExportFieldMetadataMapping fieldMapping_2 = new ExportFieldMetadataMapping();
        fieldMapping_2.setSourceField("Email");
        fieldMapping_2.setDestinationField("Email");
        fieldMapping_2.setOverwriteValue(false);

        lookupIdMap.setExportFieldMappings(Arrays.asList(fieldMapping_1, fieldMapping_2));
        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    private void registerS3LookupIdMap() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.FILE_SYSTEM);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.AWS_S3);
        lookupIdMap.setOrgId(org2);
        lookupIdMap.setOrgName("org2name");

        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }
}
