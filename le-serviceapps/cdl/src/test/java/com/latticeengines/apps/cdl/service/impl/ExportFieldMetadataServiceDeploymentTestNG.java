package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.cdl.LookupIdMapConfigValuesLookup;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AdobeAudienceManagerChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.AppNexusChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleDisplayNVideo360ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.TradeDeskChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.VerizonMediaChannelConfig;
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
    
    private List<CDLExternalSystemName> systemsToCheck = Arrays.asList(
            CDLExternalSystemName.Marketo, //
            CDLExternalSystemName.AWS_S3, //
            CDLExternalSystemName.LinkedIn, //
            CDLExternalSystemName.Facebook, //
            CDLExternalSystemName.Outreach, //
            CDLExternalSystemName.GoogleAds, //
            CDLExternalSystemName.Adobe_Audience_Mgr, //
            CDLExternalSystemName.AppNexus, //
            CDLExternalSystemName.Google_Display_N_Video_360, //
            CDLExternalSystemName.MediaMath, //
            CDLExternalSystemName.TradeDesk, //
            CDLExternalSystemName.Verizon_Media, //
            CDLExternalSystemName.Salesforce, //
            CDLExternalSystemName.Eloqua);

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateMetadata(mainTestTenant.getId(), 3);

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

        log.info(JsonUtils.serialize(play));
        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);

        Map<CDLExternalSystemName, List<ExportFieldMetadataDefaults>> defaultExportFields = new HashMap<>();
        
        for (CDLExternalSystemName system : systemsToCheck) {
            log.info("Checking default fields for system: {}", system);
            List<ExportFieldMetadataDefaults> currentDefaultField = exportFieldMetadataDefaultsService
                    .getAllAttributes(system);

            if (currentDefaultField.size() == 0) {
                currentDefaultField = createDefaultExportFields(system);
                log.info("Successfully inserted {} fields for system {}", currentDefaultField.size(), system);
            }
            
            defaultExportFields.put(system, currentDefaultField);
        }

        for (CDLExternalSystemName system : defaultExportFields.keySet()) {
            assertNotEquals(defaultExportFields.get(system).size(), 0,
                    String.format("Expected size > 0 for system %s", system));
        }
    }

    @Test(groups = "deployment-app")
    public void testMarketoLaunch() {
        registerMarketoLookupIdMap();

        createPlayLaunchChannel(new MarketoChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 4);

        long nonStandardFieldsCount = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFieldsCount, 0);
        
        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(CDLExternalSystemName.Marketo, AudienceType.CONTACTS);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testMarketoLaunch")
    public void testS3WithOutExportAttributes() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.AWS_S3;
        AudienceType audienceType = AudienceType.CONTACTS;

        registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, externalSystemName, "AWS_S3_1");

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setIncludeExportAttributes(false);
        channelConfig.setAudienceType(audienceType);
        createPlayLaunchChannel(channelConfig, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 37);

        long nonStandardFieldsCount = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        log.info("" + nonStandardFieldsCount);
        assertEquals(nonStandardFieldsCount, 30);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }
    
    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithOutExportAttributes")
    public void testS3AccountsWithOutExportAttributes() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.AWS_S3;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, externalSystemName, "AWS_S3_2");

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setAudienceType(audienceType);
        channelConfig.setIncludeExportAttributes(false);
        createPlayLaunchChannel(channelConfig, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 21);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 18);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithOutExportAttributes")
    public void testS3WithExportAttributes() {
        registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, CDLExternalSystemName.AWS_S3, "AWS_S3_3",
                null,
                null,
                InterfaceName.ContactId.name());

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setIncludeExportAttributes(true);
        createPlayLaunchChannel(channelConfig, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 144);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        assertEquals(nonStandardFields.size(), 30);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithExportAttributes")
    public void testLinkedInAccountsLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.LinkedIn;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "LinkedIn");

        LinkedInChannelConfig linkedInChannel = new LinkedInChannelConfig();
        linkedInChannel.setAudienceType(audienceType);
        createPlayLaunchChannel(linkedInChannel, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 14);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 3);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }
    
    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithExportAttributes")
    public void testLinkedInContactsLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.LinkedIn;
        AudienceType audienceType = AudienceType.CONTACTS;

        registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "LinkedIn");

        LinkedInChannelConfig linkedInChannel = new LinkedInChannelConfig();
        linkedInChannel.setAudienceType(audienceType);
        createPlayLaunchChannel(linkedInChannel, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 13);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 9);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testMarketoLaunch")
    public void testOutreachContactsLaunch() {

        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Outreach;
        AudienceType audienceType = AudienceType.CONTACTS;

        OutreachChannelConfig outreachChannel = new OutreachChannelConfig();
        outreachChannel.setAudienceType(audienceType);
        createPlayLaunchChannel(outreachChannel, registerOutreachLookupIdMap());
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(externalSystemName);
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        // ProspectOwner + AccountID + 3 mapped fields
        assertEquals(columnMetadata.size(), 5);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));

        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 0);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testMarketoLaunch")
    public void testOutreachAccountsLaunch() {

        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Outreach;
        AudienceType audienceType = AudienceType.ACCOUNTS;
        registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Outreach");

        OutreachChannelConfig outreachChannel = new OutreachChannelConfig();
        outreachChannel.setAudienceType(audienceType);
        createPlayLaunchChannel(outreachChannel, lookupIdMap);
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(externalSystemName);
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        // Account Name
        assertEquals(columnMetadata.size(), 1);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));

        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 0);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testOutreachContactsLaunch")
    public void testFacebookLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Facebook;
        AudienceType audienceType = AudienceType.CONTACTS;

        registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "Facebook");

        createPlayLaunchChannel(new FacebookChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 11);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        log.info(JsonUtils.serialize(nonStandardFields));
        assertEquals(nonStandardFields.size(), 8);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testFacebookLaunch")
    public void testEloquaLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Eloqua;
        AudienceType audienceType = AudienceType.CONTACTS;

        registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Eloqua", null,
                null,
                InterfaceName.ContactId.name());

        createPlayLaunchChannel(new EloquaChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 40);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        log.info(JsonUtils.serialize(nonStandardFields));
        assertEquals(nonStandardFields.size(), 34);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testEloquaLaunch")
    public void testSalesforceLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Salesforce;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Salesforce", null,
                null,
                InterfaceName.ContactId.name());

        createPlayLaunchChannel(new SalesforceChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 40);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        log.info(JsonUtils.serialize(nonStandardFields));
        assertEquals(nonStandardFields.size(), 34);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testSalesforceLaunch")
    public void testGoogleLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.GoogleAds;
        AudienceType audienceType = AudienceType.CONTACTS;

        registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "GoogleAds");

        createPlayLaunchChannel(new GoogleChannelConfig(), lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 10);

        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 8);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testGoogleLaunch")
    public void testLiveRampLaunch() {
        testLiveRampChannel(new AdobeAudienceManagerChannelConfig());
        testLiveRampChannel(new AppNexusChannelConfig());
        testLiveRampChannel(new GoogleDisplayNVideo360ChannelConfig());
        testLiveRampChannel(new MediaMathChannelConfig());
        testLiveRampChannel(new TradeDeskChannelConfig());
        testLiveRampChannel(new VerizonMediaChannelConfig());
    }

    private void testLiveRampChannel(ChannelConfig channelConfig) {
        registerLookupIdMap(CDLExternalSystemType.ADS, channelConfig.getSystemName(),
                channelConfig.getSystemName().toString());

        createPlayLaunchChannel(channelConfig, lookupIdMap);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(fieldMetadataService.getClass().getName());
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 1);
    }

    private List<ExportFieldMetadataDefaults> createDefaultExportFields(CDLExternalSystemName systemName) {
        String filePath = String.format("service/impl/%s_default_export_fields.json",
                systemName.toString().toLowerCase());
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filePath);
        List<ExportFieldMetadataDefaults> defaultExportFields = JsonUtils
                .convertList(JsonUtils.deserialize(inputStream, List.class), ExportFieldMetadataDefaults.class);
        exportFieldMetadataDefaultsService.createDefaultExportFields(defaultExportFields);
        return defaultExportFields;
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

        channel = playLaunchChannelService.create(play.getName(), channel);
    }

    private void registerMarketoLookupIdMap() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMap.setOrgId(org1);
        lookupIdMap.setOrgName("org1name");
        lookupIdMap.setContactId(InterfaceName.ContactId.name());

        ExportFieldMetadataMapping fieldMapping_1 = new ExportFieldMetadataMapping();
        fieldMapping_1.setSourceField(InterfaceName.CompanyName.name());
        fieldMapping_1.setDestinationField("company");
        fieldMapping_1.setOverwriteValue(false);

        ExportFieldMetadataMapping fieldMapping_2 = new ExportFieldMetadataMapping();
        fieldMapping_2.setSourceField(InterfaceName.Email.name());
        fieldMapping_2.setDestinationField("email");
        fieldMapping_2.setOverwriteValue(false);

        ExportFieldMetadataMapping fieldMapping_3 = new ExportFieldMetadataMapping();
        fieldMapping_3.setSourceField(InterfaceName.PhoneNumber.name());
        fieldMapping_3.setDestinationField("phone");
        fieldMapping_3.setOverwriteValue(false);

        lookupIdMap.setExportFieldMappings(Arrays.asList(fieldMapping_1, fieldMapping_2, fieldMapping_3));
        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    private LookupIdMap registerOutreachLookupIdMap() {
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Outreach);
        lookupIdMap.setOrgId(org1 + "outreach");
        lookupIdMap.setOrgName("org1nameOutreach");

        ExportFieldMetadataMapping fieldMapping_1 = new ExportFieldMetadataMapping();
        fieldMapping_1.setSourceField(InterfaceName.CompanyName.name());
        fieldMapping_1.setDestinationField("company");
        fieldMapping_1.setOverwriteValue(false);

        ExportFieldMetadataMapping fieldMapping_2 = new ExportFieldMetadataMapping();
        fieldMapping_2.setSourceField(InterfaceName.Email.name());
        fieldMapping_2.setDestinationField("email");
        fieldMapping_2.setOverwriteValue(false);

        ExportFieldMetadataMapping fieldMapping_3 = new ExportFieldMetadataMapping();
        fieldMapping_3.setSourceField(InterfaceName.PhoneNumber.name());
        fieldMapping_3.setDestinationField("phone");
        fieldMapping_3.setOverwriteValue(false);

        lookupIdMap.setProspectOwner(InterfaceName.Website.name());
        lookupIdMap.setAccountId(InterfaceName.AccountId.name());
        lookupIdMap.setExportFieldMappings(Arrays.asList(fieldMapping_1, fieldMapping_2, fieldMapping_3));
        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);

        return lookupIdMap;
    }

    private void registerLookupIdMap(CDLExternalSystemType systemType, CDLExternalSystemName systemName, String orgName,
            String accountId, String prospectOwner, String contactId) {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(systemType);
        lookupIdMap.setExternalSystemName(systemName);
        lookupIdMap.setOrgId(orgName + "_" + CURRENT_TIME_MILLIS);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap.setAccountId(accountId);
        lookupIdMap.setProspectOwner(prospectOwner);
        lookupIdMap.setContactId(contactId);
        lookupIdMap.setConfigValues(createLookupIdMapConfigValues(systemName, orgName));

        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    private void registerLookupIdMap(CDLExternalSystemType systemType, CDLExternalSystemName systemName,
            String orgName) {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(systemType);
        lookupIdMap.setExternalSystemName(systemName);
        lookupIdMap.setOrgId(orgName + "_" + CURRENT_TIME_MILLIS);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap.setConfigValues(createLookupIdMapConfigValues(systemName, orgName));
        
        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    private Map<String, String> createLookupIdMapConfigValues(CDLExternalSystemName systemName, String orgName) {
        Map<String, String> configValues = new HashMap<>();
        if (LookupIdMapConfigValuesLookup.containsEndDestinationIdKey(systemName)) {
            String endDestIdKey = LookupIdMapConfigValuesLookup.getEndDestinationIdKey(systemName);
            String endDestIdValue = orgName + "_" + CURRENT_TIME_MILLIS;
            configValues.put(endDestIdKey, endDestIdValue);
        }
        return configValues;
    }

    private void compareEntityInMetadata(
            List<ColumnMetadata> columnMetadataList,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        Map<String, ColumnMetadata> columnMetadataMap = new HashMap<String, ColumnMetadata>();

        columnMetadataList.forEach(columnMetadataField -> {
            columnMetadataMap.put(columnMetadataField.getAttrName(), columnMetadataField);
        });

        exportFieldMetadataList.forEach(metadataField -> {
            if (columnMetadataMap.containsKey(metadataField.getAttrName())) {
                ColumnMetadata columnMetadataToCompare = columnMetadataMap.get(metadataField.getAttrName());

                assertEquals(columnMetadataToCompare.getEntity(), metadataField.getEntity(),
                        String.format("Expected entity type %s but got %s for attribute %s",
                                columnMetadataToCompare.getEntity(), metadataField.getEntity(),
                                columnMetadataToCompare.getAttrName()));
            }
        });
    }

}
