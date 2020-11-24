package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
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
    private ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

    @Inject
    private SegmentService segmentService;

    private Set<String> defaultMarketoFields = new HashSet<>(Arrays.asList(InterfaceName.CompanyName.name(),
            InterfaceName.Email.name(), InterfaceName.PhoneNumber.name()));

    private Set<String> defaultS3Fields = new HashSet<>(Collections.singletonList("PLAY_ID"));

    private List<ExportFieldMetadataDefaults> defaultMarketoExportFields;
    private List<ExportFieldMetadataDefaults> defaultS3ExportFields;
    private List<ExportFieldMetadataDefaults> defaultLinkedInExportFields;
    private List<ExportFieldMetadataDefaults> defaultFacebookExportFields;
    private List<ExportFieldMetadataDefaults> defaultOutreachExportFields;
    private List<ExportFieldMetadataDefaults> defaultGoogleExportFields;
    private List<ExportFieldMetadataDefaults> defaultAdobeAudienceManagerExportFields;
    private List<ExportFieldMetadataDefaults> defaultAppNexusExportFields;
    private List<ExportFieldMetadataDefaults> defaultGoogleDNV360ExportFields;
    private List<ExportFieldMetadataDefaults> defaultMediaMathExportFields;
    private List<ExportFieldMetadataDefaults> defaultTradeDeskExportFields;
    private List<ExportFieldMetadataDefaults> defaultVerizonMediaExportFields;

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

        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);

        defaultMarketoExportFields = exportFieldMetadataDefaultsService.getAllAttributes(CDLExternalSystemName.Marketo);

        if (defaultMarketoExportFields.size() == 0) {
            defaultMarketoExportFields = createDefaultExportFields(CDLExternalSystemName.Marketo);
        }

        defaultS3ExportFields = exportFieldMetadataDefaultsService.getAllAttributes(CDLExternalSystemName.AWS_S3);

        if (defaultS3ExportFields.size() == 0) {
            defaultS3ExportFields = createDefaultExportFields(CDLExternalSystemName.AWS_S3);
        }

        defaultLinkedInExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.LinkedIn);

        if (defaultLinkedInExportFields.size() == 0) {
            defaultLinkedInExportFields = createDefaultExportFields(CDLExternalSystemName.LinkedIn);
        }

        defaultFacebookExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.Facebook);

        if (defaultFacebookExportFields.size() == 0) {
            defaultFacebookExportFields = createDefaultExportFields(CDLExternalSystemName.Facebook);
        }

        defaultOutreachExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.Outreach);

        if (defaultOutreachExportFields.size() == 0) {
            defaultOutreachExportFields = createDefaultExportFields(CDLExternalSystemName.Outreach);
        }

        defaultGoogleExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.GoogleAds);

        if (defaultGoogleExportFields.size() == 0) {
            defaultGoogleExportFields = createDefaultExportFields(CDLExternalSystemName.GoogleAds);
        }

        defaultAdobeAudienceManagerExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.Adobe_Audience_Mgr);

        if (defaultAdobeAudienceManagerExportFields.size() == 0) {
            defaultAdobeAudienceManagerExportFields = createDefaultExportFields(
                    CDLExternalSystemName.Adobe_Audience_Mgr);
        }

        defaultAppNexusExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.AppNexus);

        if (defaultAppNexusExportFields.size() == 0) {
            defaultAppNexusExportFields = createDefaultExportFields(CDLExternalSystemName.AppNexus);
        }

        defaultGoogleDNV360ExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.Google_Display_N_Video_360);

        if (defaultGoogleDNV360ExportFields.size() == 0) {
            defaultGoogleDNV360ExportFields = createDefaultExportFields(
                    CDLExternalSystemName.Google_Display_N_Video_360);
        }

        defaultMediaMathExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.MediaMath);

        if (defaultMediaMathExportFields.size() == 0) {
            defaultMediaMathExportFields = createDefaultExportFields(CDLExternalSystemName.MediaMath);
        }

        defaultTradeDeskExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.TradeDesk);

        if (defaultTradeDeskExportFields.size() == 0) {
            defaultTradeDeskExportFields = createDefaultExportFields(CDLExternalSystemName.TradeDesk);
        }

        defaultVerizonMediaExportFields = exportFieldMetadataDefaultsService
                .getAllAttributes(CDLExternalSystemName.Verizon_Media);

        if (defaultVerizonMediaExportFields.size() == 0) {
            defaultVerizonMediaExportFields = createDefaultExportFields(CDLExternalSystemName.Verizon_Media);
        }

        assertNotEquals(defaultMarketoExportFields.size(), 0);
        assertNotEquals(defaultS3ExportFields.size(), 0);
        assertNotEquals(defaultLinkedInExportFields.size(), 0);
        assertNotEquals(defaultFacebookExportFields.size(), 0);
        assertNotEquals(defaultOutreachExportFields.size(), 0);
        assertNotEquals(defaultGoogleExportFields.size(), 0);
        assertNotEquals(defaultAdobeAudienceManagerExportFields.size(), 0);
        assertNotEquals(defaultAppNexusExportFields.size(), 0);
        assertNotEquals(defaultGoogleDNV360ExportFields.size(), 0);
        assertNotEquals(defaultMediaMathExportFields.size(), 0);
        assertNotEquals(defaultTradeDeskExportFields.size(), 0);
        assertNotEquals(defaultVerizonMediaExportFields.size(), 0);

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
    public void testOutreachLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Outreach;
        AudienceType audienceType = AudienceType.CONTACTS;

        OutreachChannelConfig outreachChannel = new OutreachChannelConfig();
        createPlayLaunchChannel(outreachChannel, registerOutreachLookupIdMap());
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        // ProspectOwner + AccountID + 3 mapped fields
        assertEquals(columnMetadata.size(), 5);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());

        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 0);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testOutreachLaunch")
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

        assertEquals(columnMetadata.size(), 0);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testEloquaLaunch")
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
