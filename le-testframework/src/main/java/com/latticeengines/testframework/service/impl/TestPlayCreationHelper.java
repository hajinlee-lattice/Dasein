package com.latticeengines.testframework.service.impl;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.TestException;

import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.EloquaChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.GoogleChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.cdl.TalkingPointProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.proxy.objectapi.EntityProxyImpl;
import com.latticeengines.proxy.objectapi.RatingProxyImpl;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

@Component
public class TestPlayCreationHelper {

    private static final Logger log = LoggerFactory.getLogger(TestPlayCreationHelper.class);

    @Inject
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private EntityProxyImpl entityProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    public static final String SEGMENT_NAME = NamingUtils.timestamp("Segment");

    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String ACT_ATTR_PREMIUM_MARKETING_PRESCREEN = "PREMIUM_MARKETING_PRESCREEN";

    private RatingEngine ratingEngine;
    private String playName;
    private String tenantIdentifier;
    private Tenant tenant;
    private String customerSpace;
    private MetadataSegment segment;
    private MetadataSegment playTargetSegment;
    private ModelSummary modelSummary;
    private RatingEngine ruleBasedRatingEngine;
    private RatingEngine crossSellRatingEngine;
    private Play play;
    private PlayLaunch playLaunch;
    private List<PlayType> playTypes;
    private List<LookupIdMap> connections = new ArrayList<>();

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    // Todo 1: Remove testCrud from here, this class should only be a helper class
    // no testing
    // Todo 2: Make it stateless
    // Todo 3: Make it less monolithic

    public void setupTenantAndData() {
        setupTenantAndData(null);
    }

    public void setupTenantAndData(TestPlaySetupConfig plConfig) {
        if (plConfig != null && StringUtils.isNotBlank(plConfig.getExistingTenantName())) {
            useExistingTenant(plConfig.getExistingTenantName());
        } else {
            log.info("Creating new Tenant");
            Map<String, Boolean> featureFlags = plConfig != null ? plConfig.getFeatureFlags() : null;
            tenant = deploymentTestBed.bootstrapForProduct(LatticeProduct.CG, featureFlags);
            tenantIdentifier = tenant.getId();
            customerSpace = CustomerSpace.parse(tenantIdentifier).getTenantId();
            cdlTestDataService.populateData(tenantIdentifier, 3);
            postInitializeTenantCreation(tenantIdentifier);
        }
    }

    private void useExistingTenant(String tenantName) {
        log.info("Reusing Existing Tenant and Data from Redshift: " + tenantName);
        Tenant tenant = deploymentTestBed.useExistingTenantAsMain(tenantName);
        postInitializeTenantCreation(tenant.getId());
    }

    private void postInitializeTenantCreation(String fullTenantId) {
        tenant = tenantEntityMgr.findByTenantId(fullTenantId);
        log.info("Tenant = " + tenant.getId());
        tenantIdentifier = tenant.getId();
        customerSpace = CustomerSpace.parse(tenantIdentifier).getTenantId();
        MultiTenantContext.setTenant(tenant);
        deploymentTestBed.switchToSuperAdmin(tenant);
    }

    public void setupTenantAndCreatePlay(TestPlaySetupConfig testPlaySetupConfig) {
        if (StringUtils.isNotBlank(testPlaySetupConfig.getExistingTenantName())) {
            useExistingTenant(testPlaySetupConfig.getExistingTenantName());
        } else {
            setupTenantAndData(testPlaySetupConfig);
        }

        // setup Lattice_S3 lookupIdMapping
        lookupIdMappingProxy.getLookupIdsMapping(CustomerSpace.parse(tenant.getId()).getTenantId(), null, null, false);

        if (CollectionUtils.isEmpty(testPlaySetupConfig.getChannels())) {
            // Create default channel
            testPlaySetupConfig.getChannels()
                    .add(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                            .destinationSystemName(CDLExternalSystemName.Salesforce)
                            .destinationSystemId("ID_" + System.currentTimeMillis()).build());
        }

        createLookupIdMapping(testPlaySetupConfig);
        setupTestSegment();
        setupTestRulesBasedModel();

        if (testPlaySetupConfig.isMockRatingTable()) {
            cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), ratingEngine.getId(), null);
        }

        createDefaultPlayAndTestCrud(testPlaySetupConfig);
        createPlayLaunch(testPlaySetupConfig);

        Assert.assertNotNull(play);
        Assert.assertNotNull(playLaunch);

        if (testPlaySetupConfig.isLaunchPlay()) {
            launchPlayWorkflow(testPlaySetupConfig);
        }
    }

    public void setupTestSegment() {
        Restriction dynRest = createBucketRestriction(1, ComparisonType.EQUAL, BusinessEntity.Account,
                ACT_ATTR_PREMIUM_MARKETING_PRESCREEN);
        Restriction accountRestriction = createAccountRestriction(dynRest);
        Restriction contactRestriction = createContactRestriction();

        log.info("Creating test segment for test tenant: " + tenant.getId());
        segment = createSegment(SEGMENT_NAME, accountRestriction, contactRestriction);

        // todo: remove this from here
        playTargetSegment = createPlayTargetSegment();
    }

    public void setupTestRulesBasedModel() {
        if (segment == null) {
            throw new TestException("Segment not generated yet, model can only be created after creating a segment");
        }

        RatingRule ratingRule = createRatingRule();
        log.info("Creating test rules based model for test tenant: " + tenant.getId());
        ruleBasedRatingEngine = createRatingEngine(segment, ratingRule);
    }

    public Play createPlayOnlyAndGet() {
        Play createdPlay1 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlayObject());
        playName = createdPlay1.getName();
        play = createdPlay1;
        assertRulesBasedPlay(createdPlay1);
        return createdPlay1;
    }

    private Play createDefaultPlayObject() {
        if (playTargetSegment == null) {
            throw new TestException("Target segment for a play not created");
        }

        if (ratingEngine == null) {
            throw new TestException("Rating engine not created");
        }

        Play play = new Play();
        if (CollectionUtils.isEmpty(playTypes)) {
            playTypes = playProxy.getPlayTypes(tenant.getId());
        }
        play.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        play.setUpdatedBy(CREATED_BY);
        play.setPlayType(playTypes.get(0));
        play.setDisplayName("TestPlay_" + new Date().toString());
        play.setDescription("TestPlay description");
        RatingEngine ratingEngine1 = new RatingEngine();
        ratingEngine1.setId(ratingEngine.getId());
        play.setRatingEngine(ratingEngine1);

        MetadataSegment playSegment = new MetadataSegment();
        playSegment.setName(playTargetSegment.getName());
        play.setTargetSegment(playSegment);

        return play;
    }

    public void createPlayLaunch(TestPlaySetupConfig testPlaySetupConfig) {
        playLaunch = playProxy.createPlayLaunch(tenant.getId(), playName, preparePlayLaunchObject(testPlaySetupConfig));
        assertPlayLaunch(playLaunch);
    }

    public List<LookupIdMap> createLookupIdMapping(TestPlaySetupConfig testPlaySetupConfig) {
        List<LookupIdMap> createdLookups = new ArrayList<>();

        for (TestPlayChannelConfig config : testPlaySetupConfig.getChannels()) {
            LookupIdMap lookupIdMap = new LookupIdMap();
            lookupIdMap.setTenant(tenant);
            lookupIdMap.setExternalSystemType(config.getDestinationSystemType());
            lookupIdMap.setExternalSystemName(config.getDestinationSystemName());
            lookupIdMap.setOrgName("OrgName_" + new Date().toString());
            lookupIdMap.setOrgId(config.getDestinationSystemId());
            ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
            extSysAuth.setTrayAuthenticationId(config.getTrayAuthenticationId());
            extSysAuth.setTrayWorkflowEnabled(true);
            extSysAuth.setSolutionInstanceId(UUID.randomUUID().toString());
            lookupIdMap.setExternalAuthentication(extSysAuth);
            lookupIdMap = lookupIdMappingProxy.registerExternalSystem(tenant.getId(), lookupIdMap);
            createdLookups.add(lookupIdMap);
        }
        connections.addAll(createdLookups);
        return createdLookups;
    }

    public PlayLaunch launchPlayWorkflow(TestPlaySetupConfig testPlaySetupConfig, boolean useSpark) {
        playLaunch = playProxy.launchPlay(tenant.getId(), playName, playLaunch.getLaunchId(),
                testPlaySetupConfig.isPlayLaunchDryRun(), useSpark);
        if (testPlaySetupConfig.isPlayLaunchDryRun()) {
            Assert.assertNull(playLaunch.getApplicationId());
        } else {
            Assert.assertNotNull(playLaunch.getApplicationId());
        }
        return playLaunch;
    }

    public PlayLaunch launchPlayWorkflow(TestPlaySetupConfig testPlaySetupConfig) {
        return launchPlayWorkflow(testPlaySetupConfig, false);
    }

    private PlayLaunch preparePlayLaunchObject(TestPlaySetupConfig testPlaySetupConfig) {
        TestPlayChannelConfig testPlayChannelConfig = testPlaySetupConfig.getSinglePlayLaunchChannelConfig();
        PlayLaunch playLaunch = new PlayLaunch();

        playLaunch.setBucketsToLaunch(
                testPlayChannelConfig.getBucketsToLaunch() != null ? testPlayChannelConfig.getBucketsToLaunch()
                        : (new HashSet<>(Arrays.asList(RatingBucketName.values()))));
        playLaunch.setDestinationOrgId(
                testPlayChannelConfig.getDestinationSystemId() != null ? testPlayChannelConfig.getDestinationSystemId()
                        : "DummyOrgId" + System.currentTimeMillis()); // default
        playLaunch.setDestinationSysType(testPlayChannelConfig.getDestinationSystemType() != null
                ? testPlayChannelConfig.getDestinationSystemType()
                : CDLExternalSystemType.CRM); // default
        playLaunch.setDestinationAccountId(InterfaceName.SalesforceAccountID.name());
        playLaunch.setExcludeItemsWithoutSalesforceId(testPlayChannelConfig.isExcludeItemsWithoutSalesforceId());
        playLaunch.setLaunchUnscored(true);
        playLaunch.setTopNCount(testPlayChannelConfig.getTopNCount());
        playLaunch.setAudienceId(testPlayChannelConfig.getAudienceId());
        playLaunch.setCreatedBy(CREATED_BY);
        playLaunch.setUpdatedBy(CREATED_BY);
        playLaunch.setChannelConfig(createChannelConfig(testPlayChannelConfig));
        return playLaunch;
    }

    private ChannelConfig createChannelConfig(TestPlayChannelConfig testPlayChannelConfig) {
        ChannelConfig config;
        switch (testPlayChannelConfig.getDestinationSystemName()) {
        case Salesforce:
            config = new SalesforceChannelConfig();
            break;
        case Marketo:
            config = new MarketoChannelConfig();
            break;
        case AWS_S3:
            config = new S3ChannelConfig();
            break;
        case Eloqua:
            config = new EloquaChannelConfig();
            break;
        case Facebook:
            config = new FacebookChannelConfig();
            break;
        case LinkedIn:
            config = new LinkedInChannelConfig();
            ((LinkedInChannelConfig) config).setAudienceType(testPlayChannelConfig.getAudienceType());
            break;
        case GoogleAds:
            config = new GoogleChannelConfig();
            break;
        default:
            config = new SalesforceChannelConfig();
            break;
        }
        return config;
    }

    private void assertPlayLaunch(PlayLaunch playLaunch) {
        Assert.assertNotNull(playLaunch);
        Assert.assertNotNull(playLaunch.getLaunchId());
        Assert.assertNotNull(playLaunch.getPid());
        Assert.assertNotNull(playLaunch.getUpdated());
        Assert.assertNotNull(playLaunch.getCreated());
        Assert.assertNotNull(playLaunch.getLaunchState());
        assertBucketsToLaunch(playLaunch.getBucketsToLaunch());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.UnLaunched);
    }

    private void assertBucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
        Assert.assertNotNull(playLaunch.getBucketsToLaunch());
        Set<RatingBucketName> defaultBucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));
        // Assert.assertEquals(bucketsToLaunch.size(),
        // defaultBucketsToLaunch.size());
        for (RatingBucketName bucket : bucketsToLaunch) {
            Assert.assertTrue(defaultBucketsToLaunch.contains(bucket));
        }
    }

    public void createDefaultPlayAndTestCrud(TestPlaySetupConfig testPlaySetupConfig) {
        List<Play> playList = playProxy.getPlays(tenant.getId(), null, null);
        int existingPlays = playList == null ? 0 : playList.size();
        Play createdPlay1 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlayObject());
        List<PlayLaunchChannel> channels = playProxy.getPlayLaunchChannels(tenant.getId(), createdPlay1.getName(),
                true);

        channels.stream().filter(c -> StringUtils.isEmpty(c.getId()))
                .forEach(c -> createChannel(testPlaySetupConfig, createdPlay1, c));

        playName = createdPlay1.getName();
        play = createdPlay1;
        assertPlay(createdPlay1);

        if (!testPlaySetupConfig.isTestPlayCrud()) {
            return;
        }

        Map<String, List<String>> dependencies = ratingEngineProxy.getRatingEngineDependencies(tenant.getId(),
                ratingEngine.getId());
        Assert.assertNotNull(dependencies);
        Assert.assertEquals(dependencies.size(), 1);
        Assert.assertNotNull(dependencies.get(CDLObjectTypes.Play.getObjectType()));
        Assert.assertEquals(dependencies.get(CDLObjectTypes.Play.getObjectType()).size(), 1);
        Assert.assertEquals(dependencies.get(CDLObjectTypes.Play.getObjectType()).get(0), play.getDisplayName());

        List<TalkingPointDTO> tps = getTestTalkingPoints(playName);
        List<TalkingPointDTO> createTPResponse = talkingPointProxy
                .createOrUpdate(CustomerSpace.parse(tenant.getId()).toString(), tps);
        Assert.assertNotNull(createTPResponse);

        Play createdPlay2 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlayObject());
        Assert.assertNotNull(createdPlay2);
        channels = playProxy.getPlayLaunchChannels(tenant.getId(), createdPlay2.getName(), true);

        channels.stream().filter(c -> StringUtils.isEmpty(c.getId()))
                .forEach(c -> createChannel(testPlaySetupConfig, createdPlay2, c));

        dependencies = ratingEngineProxy.getRatingEngineDependencies(tenant.getId(), ratingEngine.getId());
        Assert.assertNotNull(dependencies);
        Assert.assertEquals(dependencies.size(), 1);
        Assert.assertNotNull(dependencies.get(CDLObjectTypes.Play.getObjectType()));
        Assert.assertEquals(dependencies.get(CDLObjectTypes.Play.getObjectType()).size(), 2);

        playList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), existingPlays + 2);

        playList = playProxy.getPlays(tenant.getId(), null, ratingEngine.getId());
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);

        Play retrievedPlay = playProxy.getPlay(tenant.getId(), playName);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);
        assertPlay(retrievedPlay);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = retrievedPlay;
    }

    private void createChannel(TestPlaySetupConfig config, Play play, PlayLaunchChannel channel) {
        Optional<TestPlayChannelConfig> oConfig = config.getChannels().stream()
                .filter(ch -> ch.getDestinationSystemType() == channel.getLookupIdMap().getExternalSystemType()
                        && ch.getDestinationSystemName() == channel.getLookupIdMap().getExternalSystemName())
                .findFirst();
        TestPlayChannelConfig channelConfig = oConfig.orElseGet(() -> new TestPlayChannelConfig.Builder().build());

        switch (channel.getLookupIdMap().getExternalSystemName()) {
        case Salesforce:
            channel.setChannelConfig(new SalesforceChannelConfig());
            break;
        case Marketo:
            channel.setChannelConfig(new MarketoChannelConfig());
            break;
        case AWS_S3:
            channel.setChannelConfig(new S3ChannelConfig());
            break;
        case Eloqua:
            channel.setChannelConfig(new EloquaChannelConfig());
            break;
        case Facebook:
            channel.setChannelConfig(new FacebookChannelConfig());
            break;
        case LinkedIn:
            channel.setChannelConfig(new LinkedInChannelConfig());
            break;
        case GoogleAds:
            channel.setChannelConfig(new GoogleChannelConfig());
            break;
        default:
            channel.setChannelConfig(new SalesforceChannelConfig());
            break;
        }
        channel.setTenant(tenant);
        channel.setTenantId(tenant.getPid());
        channel.setCreatedBy("ga_dev@lattice-engines.com");
        channel.setUpdatedBy("ga_dev@lattice-engines.com");
        channel.setPlay(play);
        channel.setLaunchType(channelConfig.getLaunchType() != null ? channelConfig.getLaunchType() : LaunchType.FULL);
        channel.setIsAlwaysOn(channelConfig.isAlwaysOn());
        channel.setBucketsToLaunch(channelConfig.getBucketsToLaunch());
        channel.setCronScheduleExpression(
                StringUtils.isNotBlank(channelConfig.getCronSchedule()) ? channelConfig.getCronSchedule()
                        : TestPlayChannelConfig.DEFAULT_CRON_EXPRESSION);
        channel.setExpirationPeriodString(StringUtils.isNotBlank(channelConfig.getExpirationPeriodString())
                ? channelConfig.getExpirationPeriodString()
                : TestPlayChannelConfig.DEFAULT_EXPIRATION);
        playProxy.createPlayLaunchChannel(tenant.getId(), play.getName(), channel, false);

    }

    private List<TalkingPointDTO> getTestTalkingPoints(String playName) {
        List<TalkingPointDTO> tps = new ArrayList<>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setName("plsTP1" + UUID.randomUUID());
        tp.setPlayName(playName);
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO tp1 = new TalkingPointDTO();

        tp1.setName("plsTP2" + UUID.randomUUID());
        tp1.setPlayName(playName);
        tp1.setOffset(2);
        tp1.setTitle("Test TP2 Title");
        tp1.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp1);

        return tps;
    }

    public MetadataSegment createSegment(String segmentName, Restriction accountRestriction,
            Restriction contactRestriction) {
        MetadataSegment newSegment = new MetadataSegment();
        newSegment.setAccountRestriction(accountRestriction);
        newSegment.setContactRestriction(contactRestriction);
        newSegment.setDisplayName(segmentName);
        MetadataSegment createdSegment = segmentProxy
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), newSegment);
        MetadataSegment retrievedSegment = segmentProxy
                .getMetadataSegmentByName(CustomerSpace.parse(tenant.getId()).toString(), createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        log.info("Created Segment with DisplayName {}, Account Count:{}, Contact Count: {}",
                retrievedSegment.getDisplayName(), retrievedSegment.getAccounts(), retrievedSegment.getContacts());
        return retrievedSegment;
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), playName);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertEquals(play.getRatingEngine().getId(), ratingEngine.getId());
        Assert.assertNotNull(play.getRatingEngine().getBucketMetadata());
        Assert.assertNotNull(play.getTargetSegment());
        Assert.assertEquals(play.getTargetSegment().getName(), playTargetSegment.getName());
        Assert.assertTrue(CollectionUtils.isNotEmpty(play.getRatingEngine().getBucketMetadata()));
    }

    private void assertRulesBasedPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), playName);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertEquals(play.getRatingEngine().getId(), ruleBasedRatingEngine.getId());
    }

    private Restriction createAccountRestriction(Restriction dynRestriction) {

        Restriction b3 = //
                createBucketRestriction(2, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "CloudTechnologies_ContactCenterManagement");
        Restriction b4 = //
                createBucketRestriction(4, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesSsl");
        Restriction b5 = //
                createBucketRestriction(3, ComparisonType.LESS_THAN, //
                        BusinessEntity.Account, "BusinessTechnologiesAnalytics");

        List<Restriction> restrictionList = new ArrayList<>(Arrays.asList(b3, b4, b5));
        if (dynRestriction != null) {
            restrictionList.add(dynRestriction);
        }

        Restriction innerLogical1 = LogicalRestriction.builder()//
                .and(restrictionList).build();
        Restriction innerLogical2 = LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();

        return LogicalRestriction.builder() //
                .and(Arrays.asList(innerLogical1, innerLogical2)).build();
    }

    private Restriction createBucketRestriction(Object val, ComparisonType comparisonType, BusinessEntity entityType,
            String attrName) {
        Bucket bucket = null;

        if (comparisonType == ComparisonType.EQUAL) {
            bucket = Bucket.valueBkt(comparisonType, Collections.singletonList(val));
        } else if (comparisonType == ComparisonType.LESS_THAN) {
            bucket = Bucket.rangeBkt(null, val);
        }
        return new BucketRestriction(new AttributeLookup(entityType, attrName), bucket);
    }

    private Restriction createContactRestriction() {
        return LogicalRestriction.builder()//
                .or(new ArrayList<>()).build();
    }

    public RatingProxy initRatingProxy() throws NoSuchFieldException, IllegalAccessException {

        RatingProxy ratingProxy = new RatingProxyImpl(null);

        Field f1 = ratingProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("initialWaitMsec");
        f1.setAccessible(true);
        f1.set(ratingProxy, 1000L);

        f1 = ratingProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("multiplier");
        f1.setAccessible(true);
        f1.set(ratingProxy, 2D);

        f1 = ratingProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("maxAttempts");
        f1.setAccessible(true);
        f1.set(ratingProxy, 10);

        return ratingProxy;
    }

    public EntityProxy initEntityProxy() throws NoSuchFieldException, IllegalAccessException {

        EntityProxy entityProxy = new EntityProxyImpl(this.entityProxy);

        Field f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("initialWaitMsec");
        f1.setAccessible(true);
        f1.set(entityProxy, 1000L);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("multiplier");
        f1.setAccessible(true);
        f1.set(entityProxy, 2D);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("maxAttempts");
        f1.setAccessible(true);
        f1.set(entityProxy, 10);

        return entityProxy;
    }

    public void cleanupArtifacts(boolean hardDelete) {
        try {
            log.info("Cleaning up play launch: " + playLaunch.getId());
            deletePlayLaunch(play.getName(), playLaunch.getId(), hardDelete);
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up play: " + play.getName());
            deletePlay(play.getName(), hardDelete);
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up Connections ");
            connections
                    .forEach(connection -> lookupIdMappingProxy.deleteLookupIdMap(customerSpace, connection.getId()));
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up rating engine: " + play.getRatingEngine().getId());
            play.getRatingEngine().setStatus(RatingEngineStatus.INACTIVE);
            ratingEngineProxy.createOrUpdateRatingEngine(CustomerSpace.parse(tenantIdentifier).toString(),
                    play.getRatingEngine());
            ratingEngineProxy.deleteRatingEngine(CustomerSpace.parse(tenantIdentifier).toString(),
                    play.getRatingEngine().getId(), hardDelete, null);
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up segment: " + segment.getName());
            segmentProxy.deleteSegmentByName(tenantIdentifier, segment.getName(), hardDelete);
        } catch (Exception ex) {
            ignoreException(ex);
        }

    }

    public void deletePlay(String playName, boolean hardDelete) {
        playProxy.deletePlay(tenant.getId(), playName, hardDelete);
    }

    public void deletePlayLaunch(String playName, String playLaunchId, boolean hardDelete) {
        playProxy.deletePlayLaunch(tenant.getId(), playName, playLaunchId, hardDelete);
    }

    private void ignoreException(Exception ex) {
        log.info("Could not cleanup artifact. Ignoring exception: ", ex);
    }

    private RatingRule createRatingRule() {
        RatingRule ratingRule = new RatingRule();
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = populateBucketToRuleMap();
        ratingRule.setBucketToRuleMap(bucketToRuleMap);
        // ratingRule.setDefaultBucketName(RatingBucketName.C.name());
        return ratingRule;
    }

    public RatingEngine createRatingEngine(MetadataSegment retrievedSegment, RatingRule ratingRule) {
        log.info("Creating Rating Engine");
        RatingEngine ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine1.setUpdatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setStatus(RatingEngineStatus.ACTIVE);

        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());

        ratingEngineProxy.setScoringIteration(tenant.getId(), createdRatingEngine.getId(),
                createdRatingEngine.getLatestIteration().getId(), null, null);
        createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine1);
        Assert.assertNotNull(createdRatingEngine.getScoringIteration());

        RatingEngine re = new RatingEngine();
        re.setId(createdRatingEngine.getId());
        re.setPublishedIteration(ratingEngineProxy.getRatingModel(tenant.getId(), createdRatingEngine.getId(),
                createdRatingEngine.getScoringIteration().getId()));
        createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), re);
        Assert.assertNotNull(createdRatingEngine.getPublishedIteration());

        cdlTestDataService.mockRatingTableWithSingleEngine(tenant.getId(), createdRatingEngine.getId(), null);
        ratingEngine1.setId(createdRatingEngine.getId());

        List<RatingModel> models = ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngine1.getId());
        for (RatingModel model : models) {
            if (model instanceof RuleBasedModel) {
                ((RuleBasedModel) model).setRatingRule(ratingRule);
                ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngine1.getId(), model.getId(), model);
            }
        }

        ratingEngine1 = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngine1.getId());
        ratingEngine = ratingEngine1;
        return ratingEngine1;
    }

    public RatingEngine createCrossSellRatingEngineWithPublishedRating(MetadataSegment retrievedSegment) {
        crossSellRatingEngine = new RatingEngine();
        crossSellRatingEngine.setSegment(retrievedSegment);
        crossSellRatingEngine.setCreatedBy(CREATED_BY);
        crossSellRatingEngine.setUpdatedBy(CREATED_BY);
        crossSellRatingEngine.setType(RatingEngineType.CROSS_SELL);

        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(),
                crossSellRatingEngine);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());

        try {
            modelSummary = createModelSummary(String.format("ms__%s__LETest", UUID.randomUUID().toString()), tenant);
            // modelSummaryEntityMgr.create(modelSummary);
        } catch (Exception e) {
            Assert.fail("Failed to create ModelSummary for the test");
        }

        AIModel aiModel = (AIModel) createdRatingEngine.getLatestIteration();
        aiModel.setModelingJobId(modelSummary.getApplicationId());
        aiModel.setModelingJobStatus(JobStatus.COMPLETED);
        aiModel.setModelSummaryId(modelSummary.getId());
        aiModel.setPredictionType(PredictionType.EXPECTED_VALUE);
        ratingEngineProxy.updateRatingModel(tenant.getId(), createdRatingEngine.getId(), aiModel.getId(), aiModel);

        ratingEngineProxy.setScoringIteration(tenant.getId(), createdRatingEngine.getId(), aiModel.getId(),
                getBucketMetadata(createdRatingEngine, modelSummary), "ga_dev@lattice-engines.com");
        createdRatingEngine = ratingEngineProxy.getRatingEngine(tenant.getId(), createdRatingEngine.getId());

        RatingEngine toPub = new RatingEngine();
        toPub.setId(createdRatingEngine.getId());
        toPub.setStatus(RatingEngineStatus.ACTIVE);
        toPub.setPublishedIteration(createdRatingEngine.getScoringIteration());
        ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), toPub);

        return ratingEngineProxy.getRatingEngine(tenant.getId(), createdRatingEngine.getId());
    }

    private ModelSummary createModelSummary(String modelId, Tenant tenant) throws Exception {
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setId(modelId);
        modelSummary.setDisplayName(modelId);
        modelSummary.setName(modelId);
        modelSummary.setApplicationId("application_1527712195731_0000");
        modelSummary.setRocScore(0.75);
        modelSummary.setLookupId("TENANT1|Q_EventTable_TENANT1|abcde");
        modelSummary.setTrainingRowCount(8000L);
        modelSummary.setTestRowCount(2000L);
        modelSummary.setTotalRowCount(10000L);
        modelSummary.setTrainingConversionCount(80L);
        modelSummary.setTestConversionCount(20L);
        modelSummary.setTotalConversionCount(100L);
        modelSummary.setConstructionTime(System.currentTimeMillis());
        if (modelSummary.getConstructionTime() == null) {
            modelSummary.setConstructionTime(System.currentTimeMillis());
        }
        modelSummary.setModelType(ModelType.PYTHONMODEL.getModelType());
        modelSummary.setLastUpdateTime(modelSummary.getConstructionTime());
        setDetails(modelSummary);
        modelSummary.setTenant(tenant);
        return modelSummary;
    }

    private void setDetails(ModelSummary summary) throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/pls/functionalframework/modelsummary-marketo-UI-issue.json");
        byte[] data = IOUtils.toByteArray(modelSummaryFileAsStream);
        data = CompressionUtils.compressByteArray(data);
        KeyValue details = new KeyValue();
        details.setData(data);
        summary.setDetails(details);
    }

    private List<BucketMetadata> getBucketMetadata(RatingEngine ratingEngine, ModelSummary modelSummary) {
        List<BucketMetadata> buckets = new ArrayList<>();
        BucketMetadata bkt = new BucketMetadata(BucketName.A, 15);
        bkt.setModelSummary(modelSummary);
        bkt.setRatingEngine(ratingEngine);
        buckets.add(bkt);

        bkt = new BucketMetadata(BucketName.B, 15);
        bkt.setModelSummary(modelSummary);
        bkt.setRatingEngine(ratingEngine);
        buckets.add(bkt);

        bkt = new BucketMetadata(BucketName.C, 15);
        bkt.setModelSummary(modelSummary);
        bkt.setRatingEngine(ratingEngine);
        buckets.add(bkt);

        bkt = new BucketMetadata(BucketName.D, 15);
        bkt.setModelSummary(modelSummary);
        bkt.setRatingEngine(ratingEngine);
        buckets.add(bkt);

        return buckets;
    }

    private TreeMap<String, Map<String, Restriction>> populateBucketToRuleMap() {
        TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.A, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "G");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.C, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GTE_AND_LTE, BusinessEntity.Account, "LDC_Name", "h", "n");
        populateBucketInfo(bucketToRuleMap, true, RatingBucketName.D, FrontEndQueryConstants.ACCOUNT_RESTRICTION,
                ComparisonType.GT_AND_LT, BusinessEntity.Account, "LDC_Name", "A", "O");
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.D, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.E, FrontEndQueryConstants.ACCOUNT_RESTRICTION, null,
                null, null, null, null);
        populateBucketInfo(bucketToRuleMap, false, RatingBucketName.F, FrontEndQueryConstants.CONTACT_RESTRICTION, null,
                null, null, null, null);

        return bucketToRuleMap;
    }

    private void populateBucketInfo(TreeMap<String, Map<String, Restriction>> bucketToRuleMap,
            boolean createConcreteRestriction, RatingBucketName bucketName, String key, ComparisonType comparisonType,
            BusinessEntity entity, String attrName, Object min, Object max) {
        Map<String, Restriction> bucketInfo = bucketToRuleMap.computeIfAbsent(bucketName.name(), k -> new HashMap<>());
        Restriction info;
        if (createConcreteRestriction) {
            AttributeLookup lhs = new AttributeLookup(entity, attrName);
            Bucket rhs = Bucket.rangeBkt(min, max);
            rhs.setComparisonType(comparisonType);
            info = new BucketRestriction(lhs, rhs);
        } else {
            info = LogicalRestriction.builder() //
                    .and(new ArrayList<>()).build();
        }
        bucketInfo.put(key, info);
    }

    public GlobalAuthDeploymentTestBed getDeploymentTestBed() {
        return deploymentTestBed;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public MetadataSegment createPlayTargetSegment() {
        log.info("creating playTargetSegment");
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Collections.singletonList("Delaware"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "State"), stateBkt);
        Restriction contactRestriction = createContactRestriction();
        this.playTargetSegment = createSegment(NamingUtils.timestamp("PlayTargetSegment"), accountRestriction,
                contactRestriction);
        return playTargetSegment;
    }

    public MetadataSegment createAggregatedSegment() {
        Restriction dynRest = createBucketRestriction(3, ComparisonType.LESS_THAN, BusinessEntity.Account,
                ACT_ATTR_PREMIUM_MARKETING_PRESCREEN);
        Restriction accountRestriction = createAccountRestriction(dynRest);
        Restriction contactRestriction = createContactRestriction();
        return createSegment(NamingUtils.timestamp("AggregatedSegment"), accountRestriction, contactRestriction);
    }

    public MetadataSegment createTargetSegment() {
        Restriction dynRest = createBucketRestriction(2, ComparisonType.EQUAL, BusinessEntity.Account,
                ACT_ATTR_PREMIUM_MARKETING_PRESCREEN);
        Restriction accountRestriction = createAccountRestriction(dynRest);
        Restriction contactRestriction = createContactRestriction();
        return createSegment(NamingUtils.timestamp("TargetSegment"), accountRestriction, contactRestriction);
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public String getPlayName() {
        return playName;
    }

    public RatingEngine getRatingEngine() {
        return ratingEngine;
    }

    public RatingEngine getRulesBasedRatingEngine() {
        return ruleBasedRatingEngine;
    }

    public void publishRatingEngines(List<String> ratingIdsToPublish) {
        cdlTestDataService.mockRatingTable(tenant.getId(), ratingIdsToPublish, null);
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        MultiTenantContext.setTenant(tenant);
    }

}
