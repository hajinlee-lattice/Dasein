package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
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
import com.latticeengines.domain.exposed.metadata.ListSegment;
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

    private String org1 = "org1_" + CURRENT_TIME_MILLIS;
    private String org2 = "org1_" + CURRENT_TIME_MILLIS;
    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String NAME_FOR_LIST_SEGMENT_PLAY = "list_segment_play_" + CURRENT_TIME_MILLIS;
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
    
    @Inject
    @Spy
    private DefaultExportFieldMetadataServiceImpl defaultExportFieldMetadataServiceWithNoServingStore;

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
        MockitoAnnotations.initMocks(this);

        setupTestEnvironment();
        cdlTestDataService.populateMetadata(mainTestTenant.getId(), 3);
        MetadataSegment segment = constructSegment(PLAY_TARGET_SEGMENT_NAME);
        segment = segmentService.createOrUpdateSegment(segment);
        play = new Play();
        populatePlay(play, NAME, segment);
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

        Map<String, ColumnMetadata> emptyServingStore = new HashMap<>();
        Mockito.doReturn(emptyServingStore).when(defaultExportFieldMetadataServiceWithNoServingStore)
                .getServingMetadataMap(any(), any(), any());
    }

    private void populatePlay(Play play, String name, MetadataSegment segment){
        play.setName(name);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(mainTestTenant);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(segment);
        play.setPlayType(playTypeService.getAllPlayTypes(mainCustomerSpace).get(0));
    }

    @Test(groups = "deployment-app")
    public void testMarketoLaunch() {
        LookupIdMap lookupIdMap = registerMarketoLookupIdMap();

        PlayLaunchChannel channel = createPlayLaunchChannel(new MarketoChannelConfig(), lookupIdMap, play);

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

    @Test(groups = "deployment-app")
    public void testS3WithOutExportAttributes() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.AWS_S3;
        AudienceType audienceType = AudienceType.CONTACTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, externalSystemName,
                "AWS_S3_1");

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setIncludeExportAttributes(false);
        channelConfig.setAudienceType(audienceType);
        PlayLaunchChannel channel = createPlayLaunchChannel(channelConfig, lookupIdMap, play);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 37);

        long nonStandardFieldsCount = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFieldsCount, 30);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);
        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithOutExportAttributes")
    public void testS3AccountsWithOutExportAttributes() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.AWS_S3;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, externalSystemName,
                "AWS_S3_2");

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setAudienceType(audienceType);
        channelConfig.setIncludeExportAttributes(false);
        PlayLaunchChannel channel = createPlayLaunchChannel(channelConfig, lookupIdMap, play);

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
        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, CDLExternalSystemName.AWS_S3,
                "AWS_S3_3",
                null,
                null,
                InterfaceName.ContactId.name());

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        channelConfig.setIncludeExportAttributes(true);
        PlayLaunchChannel channel = createPlayLaunchChannel(channelConfig, lookupIdMap, play);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 144);

        List<ColumnMetadata> nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField)
                .collect(Collectors.toList());
        assertEquals(nonStandardFields.size(), 30);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testS3WithOutExportAttributes")
    public void testPlayBasedOnListSegment() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.AWS_S3;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, externalSystemName,
                "AWS_S3_4");

        S3ChannelConfig channelConfig = new S3ChannelConfig();
        Play playBasedOnListSegment = createPlayBasedOnListSegment();
        PlayLaunchChannel channel = createPlayLaunchChannel(channelConfig, lookupIdMap,
                playBasedOnListSegment);

        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());

        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info("Column metadata from play based on list segment:, {}.", JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 37);
    }

    @Test(groups = "deployment-app")
    public void testLinkedInAccounts() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.LinkedIn;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName,
                "LinkedInAccounts");

        LinkedInChannelConfig linkedInAccountsConfig = new LinkedInChannelConfig();
        linkedInAccountsConfig.setAudienceType(audienceType);
        PlayLaunchChannel accountsChannel = createPlayLaunchChannel(linkedInAccountsConfig, lookupIdMap, play);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);

        testLinkedInAccountsLaunch(accountsChannel, exportFieldMetadataList);
        testLinkedInAccountsWithNoServingStore(accountsChannel, exportFieldMetadataList);
    }

    public void testLinkedInAccountsLaunch(PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 14);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 3);

        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    public void testLinkedInAccountsWithNoServingStore(PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        List<ColumnMetadata> columnMetadata = defaultExportFieldMetadataServiceWithNoServingStore
                .getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 14);

        List<ExportFieldMetadataDefaults> expectedSubset = exportFieldMetadataList
                .stream()
                .filter(ExportFieldMetadataDefaults::getForcePopulateIfExportEnabled).collect(Collectors.toList());
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();

        assertEquals(expectedSubset.size(), 0);
        assertEquals(nonStandardFields, 14);
    }

    @Test(groups = "deployment-app")
    public void testLinkedInContacts() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.LinkedIn;
        AudienceType audienceType = AudienceType.CONTACTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName,
                "LinkedInContacts");

        LinkedInChannelConfig linkedInContactsConfig = new LinkedInChannelConfig();
        linkedInContactsConfig.setAudienceType(audienceType);
        PlayLaunchChannel contactsChannel = createPlayLaunchChannel(linkedInContactsConfig, lookupIdMap, play);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);

        testLinkedInContactsLaunch(contactsChannel, exportFieldMetadataList);
        testLinkedInContactsWithNoServingStore(contactsChannel, exportFieldMetadataList);
    }

    public void testLinkedInContactsLaunch(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 13);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 1);

        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    public void testLinkedInContactsWithNoServingStore(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        List<ColumnMetadata> columnMetadata = defaultExportFieldMetadataServiceWithNoServingStore
                .getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 13);

        List<ExportFieldMetadataDefaults> expectedSubset = exportFieldMetadataList.stream()
                .filter(ExportFieldMetadataDefaults::getForcePopulateIfExportEnabled).collect(Collectors.toList());
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();

        assertEquals(expectedSubset.size(), 10);
        assertEquals(nonStandardFields, 3);
    }

    @Test(groups = "deployment-app")
    public void testOutreachContactsLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Outreach;
        AudienceType audienceType = AudienceType.CONTACTS;

        OutreachChannelConfig outreachChannel = new OutreachChannelConfig();
        outreachChannel.setAudienceType(audienceType);
        PlayLaunchChannel channel = createPlayLaunchChannel(outreachChannel, registerOutreachLookupIdMap(), play);

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

    @Test(groups = "deployment-app")
    public void testOutreachAccountsLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Outreach;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Outreach");

        OutreachChannelConfig outreachChannel = new OutreachChannelConfig();
        outreachChannel.setAudienceType(audienceType);
        PlayLaunchChannel channel = createPlayLaunchChannel(outreachChannel, lookupIdMap, play);
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

    @Test(groups = "deployment-app")
    public void testFacebookContacts() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Facebook;
        AudienceType audienceType = AudienceType.CONTACTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "Facebook");
        PlayLaunchChannel channel = createPlayLaunchChannel(new FacebookChannelConfig(), lookupIdMap, play);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);

        testFacebookContactsLaunch(channel, exportFieldMetadataList);
        testFacebookContactsWithNoServingStore(channel, exportFieldMetadataList);
    }

    public void testFacebookContactsLaunch(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 11);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 0);

        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    public void testFacebookContactsWithNoServingStore(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        List<ColumnMetadata> columnMetadata = defaultExportFieldMetadataServiceWithNoServingStore
                .getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 11);

        List<ExportFieldMetadataDefaults> expectedSubset = exportFieldMetadataList.stream()
                .filter(ExportFieldMetadataDefaults::getForcePopulateIfExportEnabled).collect(Collectors.toList());
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();

        assertEquals(expectedSubset.size(), 10);
        assertEquals(nonStandardFields, 1);
    }

    @Test(groups = "deployment-app")
    public void testEloquaLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Eloqua;
        AudienceType audienceType = AudienceType.CONTACTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Eloqua",
                null,
                null,
                InterfaceName.ContactId.name());

        PlayLaunchChannel channel = createPlayLaunchChannel(new EloquaChannelConfig(), lookupIdMap, play);

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

    @Test(groups = "deployment-app")
    public void testSalesforceLaunch() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.Salesforce;
        AudienceType audienceType = AudienceType.ACCOUNTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.MAP, externalSystemName, "Salesforce",
                null,
                null,
                InterfaceName.ContactId.name());

        PlayLaunchChannel channel = createPlayLaunchChannel(new SalesforceChannelConfig(), lookupIdMap, play);

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

    @Test(groups = "deployment-app")
    public void testGoogleContacts() {
        CDLExternalSystemName externalSystemName = CDLExternalSystemName.GoogleAds;
        AudienceType audienceType = AudienceType.CONTACTS;

        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.ADS, externalSystemName, "GoogleAds");
        PlayLaunchChannel channel = createPlayLaunchChannel(new GoogleChannelConfig(), lookupIdMap, play);

        List<ExportFieldMetadataDefaults> exportFieldMetadataList = exportFieldMetadataDefaultsService
                .getExportEnabledAttributesForAudienceType(externalSystemName, audienceType);

        testGoogleContactsLaunch(channel, exportFieldMetadataList);
        testGoogleContactsWithNoServingStore(channel, exportFieldMetadataList);
    }

    public void testGoogleContactsLaunch(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        ExportFieldMetadataService fieldMetadataService = ExportFieldMetadataServiceBase
                .getExportFieldMetadataService(channel.getLookupIdMap().getExternalSystemName());
        List<ColumnMetadata> columnMetadata = fieldMetadataService.getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 10);

        List<String> attrNames = columnMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList());
        log.info(JsonUtils.serialize(attrNames));
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();
        assertEquals(nonStandardFields, 0);

        compareEntityInMetadata(columnMetadata, exportFieldMetadataList);
    }

    public void testGoogleContactsWithNoServingStore(
            PlayLaunchChannel channel,
            List<ExportFieldMetadataDefaults> exportFieldMetadataList) {
        List<ColumnMetadata> columnMetadata = defaultExportFieldMetadataServiceWithNoServingStore
                .getExportEnabledFields(mainCustomerSpace, channel);
        log.info(JsonUtils.serialize(columnMetadata));

        assertEquals(columnMetadata.size(), 10);

        List<ExportFieldMetadataDefaults> expectedSubset = exportFieldMetadataList.stream()
                .filter(ExportFieldMetadataDefaults::getForcePopulateIfExportEnabled).collect(Collectors.toList());
        long nonStandardFields = columnMetadata.stream().filter(ColumnMetadata::isCampaignDerivedField).count();

        assertEquals(expectedSubset.size(), 10);
        assertEquals(nonStandardFields, 0);
    }

    @Test(groups = "deployment-app")
    public void testLiveRampLaunch() {
        testLiveRampChannel(new AdobeAudienceManagerChannelConfig());
        testLiveRampChannel(new AppNexusChannelConfig());
        testLiveRampChannel(new GoogleDisplayNVideo360ChannelConfig());
        testLiveRampChannel(new MediaMathChannelConfig());
        testLiveRampChannel(new TradeDeskChannelConfig());
        testLiveRampChannel(new VerizonMediaChannelConfig());
    }

    private Play createPlayBasedOnListSegment() {
        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setDisplayName("List segment");
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem("External system");
        listSegment.setExternalSegmentId(UUID.randomUUID().toString());
        metadataSegment.setListSegment(listSegment);
        metadataSegment = segmentService.createOrUpdateListSegment(metadataSegment);
        Play play = new Play();
        populatePlay(play, NAME_FOR_LIST_SEGMENT_PLAY, metadataSegment);
        playEntityMgr.createPlay(play);
        return playEntityMgr.getPlayByName(NAME_FOR_LIST_SEGMENT_PLAY, false);
    }

    private void testLiveRampChannel(ChannelConfig channelConfig) {
        LookupIdMap lookupIdMap = registerLookupIdMap(CDLExternalSystemType.ADS,
                channelConfig.getSystemName(),
                channelConfig.getSystemName().toString());

        PlayLaunchChannel channel = createPlayLaunchChannel(channelConfig, lookupIdMap, play);

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

    private PlayLaunchChannel createPlayLaunchChannel(ChannelConfig channelConfig, LookupIdMap lookupIdMap, Play play) {
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setTenant(mainTestTenant);
        channel.setCreatedBy(CREATED_BY);
        channel.setUpdatedBy(CREATED_BY);
        channel.setLaunchType(LaunchType.FULL);
        channel.setPlay(play);
        channel.setLaunchUnscored(true);

        channel.setChannelConfig(channelConfig);
        channel.setLookupIdMap(lookupIdMap);

        channel = playLaunchChannelService.create(play.getName(), channel);

        return channel;
    }

    private LookupIdMap registerMarketoLookupIdMap() {
        LookupIdMap lookupIdMap = new LookupIdMap();
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

        return lookupIdMap;
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

    private LookupIdMap registerLookupIdMap(CDLExternalSystemType systemType, CDLExternalSystemName systemName,
            String orgName,
            String accountId, String prospectOwner, String contactId) {
        LookupIdMap lookupIdMap = new LookupIdMap();
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

        return lookupIdMap;
    }

    private LookupIdMap registerLookupIdMap(CDLExternalSystemType systemType,
            CDLExternalSystemName systemName,
            String orgName) {
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setTenant(mainTestTenant);
        lookupIdMap.setExternalSystemType(systemType);
        lookupIdMap.setExternalSystemName(systemName);
        lookupIdMap.setOrgId(orgName + "_" + CURRENT_TIME_MILLIS);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap.setConfigValues(createLookupIdMapConfigValues(systemName, orgName));
        
        lookupIdMap = lookupIdMappingService.registerExternalSystem(lookupIdMap);

        return lookupIdMap;
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
