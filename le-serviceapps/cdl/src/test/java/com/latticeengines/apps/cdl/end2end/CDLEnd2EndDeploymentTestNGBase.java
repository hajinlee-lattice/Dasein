package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.common.exposed.metric.MetricTags.Test.TAG_TEST_CLASS;
import static com.latticeengines.common.exposed.metric.MetricTags.Test.TAG_TEST_GROUP;
import static com.latticeengines.common.exposed.metric.MetricTags.Test.TAG_TEST_METHOD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.service.impl.CheckpointAutoService;
import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.util.ActivityMetricsTestUtils;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.domain.exposed.workflow.FailingStep;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelingFileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

public abstract class CDLEnd2EndDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final Logger log = LoggerFactory.getLogger(CDLEnd2EndDeploymentTestNGBase.class);

    public static final int S3_CHECKPOINTS_VERSION = 26;
    private static final int S3_RATING_CHECKPOINTS_VERSION = 25;

    private static final String INITIATOR = "test@lattice-engines.com";
    private static final String S3_VDB_DIR = "le-serviceapps/cdl/end2end/vdb";
    private static final String S3_VDB_VERSION = "2";

    protected static final String S3_CSV_DIR = "le-serviceapps/cdl/end2end/csv";
    protected static final String S3_CSV_VERSION = "6";

    private static final String S3_AVRO_DIR = "le-serviceapps/cdl/end2end/avro";
    private static final String S3_AVRO_VERSION = "6";
    static final String S3_AVRO_VERSION_ADVANCED_MATCH = "6";
    static final String ADVANCED_MATCH_SUFFIX = "EntityMatch";
    private static final String MAP_ID_PREFIX = "LETest_MapTo_";

    private static final String LARGE_CSV_DIR = "le-serviceapps/cdl/end2end/large_csv";
    private static final String LARGE_CSV_VERSION = "1";

    /* Expected account result */

    // Number of total account after ProcessAccount test
    static final Long ACCOUNT_PA = 900L;
    // Number of total account after inserting new accounts ProcessAccount test
    static final Long NEW_ACCOUNT_PA = 10L;
    // Number of total account after updating existing accounts ProcessAccount test
    static final Long UPDATE_ACCOUNT_PA = 10L;
    // Number of total account after ProcessAccount entity match test
    static final Long ACCOUNT_PA_EM = 903L; // 3 accs from contact
    // Number of total account after ProcessAccount entity match test for GA tenants
    // (implicit accounts excluded)
    static final Long ACCOUNT_PA_EMGA = 900L;
    // Number of total account after UpdateAccount test
    static final Long ACCOUNT_UA = 1000L;
    // Number of new account after UpdateAccount test
    static final Long NEW_ACCOUNT_UA = 100L;
    // Number of new account after ProcessTransaction entity match test (There
    // are 91 new CustomerAccountId in txn imports for ProcessTransaction test)
    static final Long NEW_ACCOUNT_PT_EM = 91L;
    // Number of new account after ProcessTransaction entity match test for GA
    // tenants
    static final Long NEW_ACCOUNT_PT_EMGA = 0L;
    // Number of new account after UpdateTransaction entity match test (There
    // are 190 new CustomerAccountId in txn imports for UpdateTransaction test)
    static final Long NEW_ACCOUNT_UT_EM = 190L;
    // Number of new account after UpdateTransaction entity match test for GA
    // tenants
    static final Long NEW_ACCOUNT_UT_EMGA = 0L;
    // Number of new account after UpdateAccount entity match test
    // FIXME change back to 111 after using new ProcessAccount checkpoint.
    // currently
    // it is 113 cuz one anonymous account created and one legacy account due to
    // case insensitive ID match. Anonymous account will be created by
    // ProcessAccount by one of its contact after updating checkpoint (back to
    // 111).
    static final Long NEW_ACCOUNT_UA_EM = 111L;
    // Number of total account after UpdateAccount entity match test for GA tenants
    static final Long NEW_ACCOUNT_UA_EMGA = 106L;
    // Number of updated account after UpdateAccount test
    static final Long UPDATED_ACCOUNT_UA = 100L;
    // Number of updated account after UpdateAccount entity match test
    static final Long UPDATED_ACCOUNT_UA_EM = 100L;
    // Number of updated account after UpdateAccount entity match test for GA
    // tenants
    static final Long UPDATED_ACCOUNT_UA_EMGA = 100L;

    // Number of total account after ProcessTransaction entity match test (There
    // are 91 new CustomerAccountId in txn imports for ProcessTransaction test)
    // -- 994
    static final Long ACCOUNT_PT_EM = ACCOUNT_PA_EM + NEW_ACCOUNT_PT_EM;
    // Number of total account after UpdateTransaction entity match test (There
    // are 190 new CustomerAccountId in txn imports for UpdateTransaction test)
    // -- 1184
    // Number of total account after ProcessTransaction entity match test for GA
    // tenants (use ACCOUNT_PA_EM since checkpoint updated to multi template)
    static final Long ACCOUNT_PT_EMGA = ACCOUNT_PA_EM + NEW_ACCOUNT_PT_EMGA;
    static final Long ACCOUNT_UT_EM = ACCOUNT_PT_EM + NEW_ACCOUNT_UT_EM;
    // Number of total account after UpdateTransaction entity match test for GA
    // tenants (use ACCOUNT_PT_EM since checkpoint is not changed yet)
    static final Long ACCOUNT_UT_EMGA = ACCOUNT_PA_EMGA + NEW_ACCOUNT_UT_EMGA;
    // Number of total account after UpdateAccount entity match test -- 1016
    static final Long ACCOUNT_UA_EM = ACCOUNT_PA_EM + NEW_ACCOUNT_UA_EM;

    /* Expected contact result */

    // Number of total contact after ProcessAccount test
    static final Long CONTACT_PA = 900L;
    // Number of total contact after inserting new contacts ProcessAccount test
    static final Long NEW_CONTACT_PA = 10L;
    // Number of total contact after updating existing contacts ProcessAccount test
    static final Long UPDATE_CONTACT_PA = 10L;
    // Number of total contact after ProcessAccount entity match test
    static final Long CONTACT_PA_EM = 900L;
    // Number of total contact after ProcessAccount entity match test for GA tenants
    static final Long CONTACT_PA_EMGA = 900L;
    static final Long CONTACT_PA_EMGA_SERVING = 899L; // excluding orphan contact
    // Number of total contact after UpdateContact test
    static final Long CONTACT_UC = 1000L;
    // Number of total contact after ProcessAccount entity match test
    static final Long CONTACT_UA_EM = 1005L;
    // Number of new contact after UpdateContact test
    static final Long NEW_CONTACT_UC = 100L;
    // Number of new contact after UpdateAccount entity match test
    static final Long NEW_CONTACT_UA_EM = 105L;
    // Number of new contact after UpdateAccount entity match test for GA tenants
    static final Long NEW_CONTACT_UA_EMGA = 114L;
    // Number of updated contact after UpdateContact test
    static final Long UPDATED_CONTACT_UC = 100L;
    // Number of updated contact after ProcessAccount entity match test
    static final Long UPDATED_CONTACT_UA_EM = 100L;
    // Number of updated contact after ProcessAccount entity match test for GA
    // tenants
    static final Long UPDATED_CONTACT_UA_EMGA = 100L;

    /* Expected transaction result */

    // Number of new raw txn after ProcessTransaction test
    static final Long NEW_TRANSACTION_PT = 48760L;
    // Number of new raw txn after UpdateTransaction test
    static final Long NEW_TRANSACTION_UT = 13633L;
    // Number of total raw txn after UpdateTransaction test -- 62393
    static final Long TOTAL_TRANSACTION_UT = NEW_TRANSACTION_PT + NEW_TRANSACTION_UT;
    // Number of aggregated daily transaction after ProcessTransaction test
    static final Long DAILY_TXN_PT = 41156L;
    // Number of aggregated daily transaction after ProcessTransaction entity
    // match test
    static final Long DAILY_TXN_PT_EM = 41064L;
    // Number of aggregated daily transaction after UpdateTransaction test
    static final Long DAILY_TXN_UT = 50238L;
    // Number of aggregated daily transaction after UpdateTransaction entity
    // match test (txn data distribution is different for txn test with and
    // without entity match)
    static final Long DAILY_TXN_UT_EM = 50863L;
    // Number of aggregated period transaction after ProcessTransaction test
    static final Long PERIOD_TRANSACTION_PT = 62550L;
    // Number of aggregated period transaction after ProcessTransaction entity
    // match test (txn data distribution is different for txn test with and
    // without entity match)
    static final Long PERIOD_TXN_PT_EM = 62037L;
    // Number of aggregated period transaction after UpdateTransaction test
    static final Long PERIOD_TRANSACTION_UT = 73892L;
    // Number of aggregated period transaction after UpdateTransaction entity
    // match test (txn data distribution is different for txn test with and
    // without entity match)
    static final Long PERIOD_TRANSACTION_UT_EM = 75183L;
    // Number of total purchase history attributes after ProcessTransaction test
    static final Long TOTAL_PURCHASE_HISTORY_PT = 5L;
    // Number of total purchase history attributes after UpdateTransaction test
    static final Long TOTAL_PURCHASE_HISTORY_UT = 6L;

    // Number of distinct days in daily txn store after ProcessTransaction test
    static final int DAILY_TXN_DAYS_PT = 214;
    // Number of distinct days in daily txn store after UpdateTransaction test
    static final int DAILY_TXN_DAYS_UT = 260;
    // Min date in daily txn store after ProcessTransaction test
    static final String MIN_TXN_DATE_PT = "2016-03-15";
    // Max date in daily txn store after ProcessTransaction test
    static final String MAX_TXN_DATE_PT = "2017-10-20";
    // Min date in daily txn store after UpdateTransaction test
    static final String MIN_TXN_DATE_UT = "2016-03-15";
    // Max date in daily txn store after UpdateTransaction test
    static final String MAX_TXN_DATE_UT = "2017-12-31";

    // To verify txn daily store, pick certain aid, pid and txn date
    private static final String VERIFY_DAILYTXN_TXNDATE = "2017-09-28";
    private static final String VERIFY_DAILYTXN_ACCOUNTID = "109";
    private static final String VERIFY_DAILYTXN_PRODUCTID = "650050C066EF46905EC469E9CC2921E0";
    // For verified aid, pid and txn date, daily txn amount after
    // ProcessTransaction test
    static final double VERIFY_DAILYTXN_AMOUNT_PT = 1860.;
    // For verified aid, pid and txn date, daily txn quantity after
    // ProcessTransaction test
    static final double VERIFY_DAILYTXN_QUANTITY_PT = 10;
    // For verified aid, pid and txn date, daily txn cost after
    // ProcessTransaction test
    static final double VERIFY_DAILYTXN_COST_PT = 1054.588389;
    // Number of new raw txn after first PA of legacy tenant end2end test
    static final Long TRANSACTION_LEGACY_FIRST_PA = 24367L;
    // Number of new raw txn after second PA of legacy tenant end2end test
    static final Long TRANSACTION_LEGACY_SECOND_PA = 24393L;
    // Number of total purchase history attributes after PA of legacy tenant end2end
    // test
    static final Long TOTAL_PURCHASE_HISTORY_PA = 5L;

    /* Expected product result */

    // Number of product id after ProcessAccount test
    static final Long PRODUCT_ID_PA = 40L;
    // Number of product id after ProcessLegacy test for VDB
    static final Long PRODUCT_ID_VDB_PA = 10L;
    // Number of product id after ProcessLegacy test for VDB after new import
    static final Long NEW_PRODUCT_ID_VDB_PA = 5L;
    // Number of product hierarchy after ProcessAccount test
    static final Long PRODUCT_HIERARCHY_PA = 5L;
    // Number of product bundle after ProcessAccount test
    static final Long PRODUCT_BUNDLE_PA = 14L;
    // Error message after merging product
    static final String PRODUCT_ERROR_MESSAGE = null;
    // Warn message after merging product
    static final String PRODUCT_WARN_MESSAGE = "whatever warn message as it is not null or empty string";
    // Number of products in batch store after ProcessTransaction test
    static final Long BATCH_STORE_PRODUCT_PT = 103L;
    // Number of products in serving store after ProcessTransaction test
    static final Long SERVING_STORE_PRODUCTS_PT = 34L;
    // Number of product hierarchy in serving store after ProcessTransaction
    // test
    static final Long SERVING_STORE_PRODUCT_HIERARCHIES_PT = 20L;
    // Number of products in batch store after first PA ProcessLegacy test
    static final Long BATCH_STORE_PRODUCT_PA = 10L;
    // Number of products in batch store after second PA ProcessLegacy test
    static final Long NEW_BATCH_STORE_PRODUCT_PA = 15L;
    // Number of products in serving store after first PA ProcessLegacy test
    static final Long SERVING_STORE_PRODUCTS_PA = 10L;
    // Number of products in serving store after second PA ProcessLegacy test
    static final Long NEW_SERVING_STORE_PRODUCTS_PA = 5L;

    /* Expected segment result */

    static final String SEGMENT_NAME_1 = NamingUtils.timestamp("E2ESegment1");
    static final long SEGMENT_1_ACCOUNT_1 = 21;
    static final long SEGMENT_1_CONTACT_1 = 23;
    static final long SEGMENT_1_ACCOUNT_2 = 30;
    static final long SEGMENT_1_CONTACT_2 = 32;
    static final long SEGMENT_1_ACCOUNT_3 = 54;
    static final long SEGMENT_1_CONTACT_3 = 63;
    static final long SEGMENT_1_ACCOUNT_4 = 58;
    static final long SEGMENT_1_CONTACT_4 = 68;

    static final String SEGMENT_NAME_2 = NamingUtils.timestamp("E2ESegment2");
    static final long SEGMENT_2_ACCOUNT_1 = 13;
    static final long SEGMENT_2_CONTACT_1 = 13;
    static final long SEGMENT_2_ACCOUNT_2 = 45;
    static final long SEGMENT_2_CONTACT_2 = 49;
    static final long SEGMENT_2_ACCOUNT_2_REBUILD = 44;
    static final long SEGMENT_2_CONTACT_2_REBUILD = 49;

    static final String SEGMENT_NAME_3 = NamingUtils.timestamp("E2ESegment3");
    static final long SEGMENT_3_ACCOUNT_1 = 53;
    static final long SEGMENT_3_CONTACT_1 = 53;
    static final long SEGMENT_3_ACCOUNT_2 = 60;
    static final long SEGMENT_3_CONTACT_2 = 60;
    static final long SEGMENT_3_ACCOUNT_3 = 52;
    static final long SEGMENT_3_CONTACT_3 = 52;

    static final String SEGMENT_NAME_CURATED_ATTR = NamingUtils.timestamp("E2ESegmentCuratedAttr");
    static final String SEGMENT_NAME_MODELING = NamingUtils.timestamp("E2ESegmentModeling");
    static final String SEGMENT_NAME_TRAINING = NamingUtils.timestamp("E2ESegmentTraining");
    static final String SEGMENT_NAME_PRODUCT_BUNDLE = NamingUtils.timestamp("SEGMENT_NAME_PRODUCT_BUNDLE");

    /* Expected rating result */

    static final long RATING_A_COUNT_1 = 6;
    static final long RATING_D_COUNT_1 = 5;
    static final long RATING_F_COUNT_1 = 2;

    static final String TARGET_PRODUCT = "A48F113437D354134E584D8886116989";
    static final String TRAINING_PRODUCT = "9IfG2T5joqw0CIJva0izeZXSCwON1S";

    int actionsNumber;

    @Inject
    DataCollectionProxy dataCollectionProxy;

    @Inject
    DataFeedProxy dataFeedProxy;

    @Inject
    CDLProxy cdlProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected ModelingFileUploadProxy fileUploadProxy;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected CheckpointService checkpointService;

    @Inject
    protected CheckpointAutoService checkpointAutoService;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    private TestMetadataSegmentProxy testMetadataSegmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    protected ServingStoreProxy servingStoreProxy;

    @Inject
    protected PeriodProxy periodProxy;

    @Inject
    protected ActionProxy actionProxy;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private BatonService batonService;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    protected String s3Bucket;

    @Value("${common.le.environment}")
    private String leEnv;

    protected String processAnalyzeAppId;
    protected DataCollection.Version initialVersion;

    protected RatingEngine ratingEngine;

    private ThreadLocal<Span> testSpanHolder = new ThreadLocal<>();
    private ThreadLocal<Scope> testScopeHolder = new ThreadLocal<>();

    @BeforeClass(groups = { "end2end", "manual", "precheckin", "deployment", "end2end_with_import" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
    }

    /*
     * things that should be setup by all tests (should not override this)
     */
    @BeforeClass(groups = { "end2end", "manual", "precheckin", "deployment", "end2end_with_import" })
    public final void setupShared() {
        if (!isLocalEnvironment()) {
            log.info("Enable copying checkpoint to S3");
            checkpointService.enableCopyToS3();
            checkpointAutoService.enableCopyToS3();
        }
    }

    @AfterClass(groups = { "end2end", "precheckin" })
    protected void cleanup() throws Exception {
        checkpointService.cleanup();
    }

    @BeforeMethod(groups = "end2end")
    protected void startTrace(@NotNull Method method) {
        Tracer tracer = GlobalTracer.get();
        Class<?> clz = method.getDeclaringClass();
        log.info("Starting trace for test {}#{}", clz.getSimpleName(), method.getName());
        Span testSpan = tracer.buildSpan(String.format("e2e-%s#%s", clz.getSimpleName(), method.getName()))
                .withTag(TAG_TEST_GROUP, "end2end") //
                .withTag(TAG_TEST_CLASS, clz.getSimpleName()) //
                .withTag(TAG_TEST_METHOD, method.getName()) //
                .start();
        // testing baggage item
        testSpan.setBaggageItem("testTenant", mainCustomerSpace);
        testSpanHolder.set(testSpan);

        // activate so that it will be propagated
        Scope testScope = tracer.activateSpan(testSpan);
        testScopeHolder.set(testScope);
    }

    @AfterMethod(groups = "end2end")
    protected void endTrace(@NotNull Method method) {
        Class<?> clz = method.getDeclaringClass();
        log.info("Finishing trace for test {}#{}", clz.getSimpleName(), method.getName());
        if (testScopeHolder.get() != null) {
            testScopeHolder.get().close();
            testScopeHolder.remove();
        }
        if (testSpanHolder.get() != null) {
            testSpanHolder.get().finish();
            testSpanHolder.remove();
        }
    }

    protected void setupEnd2EndTestEnvironment() throws Exception {
        setupEnd2EndTestEnvironment(null);
    }

    protected void setupEnd2EndTestEnvironment(Map<String, Boolean> featureFlagMap) throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");

        if (featureFlagMap == null) {
            featureFlagMap = new HashMap<>();
        }
        // use non entity match path by default unless its overwritten explicitly
        featureFlagMap.putIfAbsent(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        setupTestEnvironmentWithFeatureFlags(featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();

        log.info("Test environment setup finished.");
        createDataFeed();
        setupBusinessCalendar();
        setupPurchaseHistoryMetrics();
        setDefaultAPSRollupPeriod();

        attachPlsProxies();

        // If don't want to remove testing tenant for debug purpose, remove
        // comments on this line but don't check in
        // testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));

    }

    protected void setupEnd2EndTestEnvironmentByFile(String jsonFileName) {
        log.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironmentByFile(jsonFileName);
        mainTestTenant = testBed.getMainTestTenant();

        log.info("Test environment setup finished.");
        createDataFeed();
        setupBusinessCalendar();
        setupPurchaseHistoryMetrics();
        setDefaultAPSRollupPeriod();

        attachPlsProxies();
    }

    protected void attachPlsProxies() {
        attachProtectedProxy(fileUploadProxy);
        attachProtectedProxy(testMetadataSegmentProxy);
    }

    protected void resetCollection() {
        log.info("Start reset collection data ...");
        boolean resetStatus = cdlProxy.reset(mainTestTenant.getId());
        Assert.assertTrue(resetStatus);
    }

    void clearCache() {
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(tenantId, CacheName.getCdlCacheGroup());
    }

    void retryProcessAnalyze() {
        log.info("Start retrying PA ...");
        ApplicationId appId = cdlProxy.restartProcessAnalyze(mainTestTenant.getId());
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
    }

    void processAnalyzeSkipPublishToS3() {
        processAnalyzeSkipPublishToS3(null);
    }

    void processAnalyzeSkipPublishToS3(Long currentPATimestamp) {
        processAnalyzeSkipPublishToS3(currentPATimestamp, false);
    }

    void processAnalyzeSkipPublishToS3(Long currentPATimestamp, boolean fullRematch) {
        processAnalyze(getProcessRequest(currentPATimestamp, fullRematch));
    }

    void processAnalyze() {
        processAnalyze(null);
    }

    void processAnalyze(ProcessAnalyzeRequest request) {
        processAnalyze(request, JobStatus.COMPLETED);
    }

    void processAnalyze(ProcessAnalyzeRequest request, JobStatus expectedResult) {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), request);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        if (expectedResult != JobStatus.FAILED) {
            assertEquals(completedStatus, expectedResult);
        } else {
            assertTrue(completedStatus == JobStatus.FAILED || completedStatus == JobStatus.PENDING_RETRY, String.format(
                    "Completed status should be either pending retry or failed, got %s instead", completedStatus));
        }
    }

    SourceFile uploadDeleteCSV(String fileName, SchemaInterpretation schema, CleanupOperationType type,
            Resource source) {
        log.info("Upload file " + fileName + ", operation type is " + type.name() + ", Schema is " + schema.name());
        return fileUploadProxy.uploadDeleteFile(false, fileName, schema.name(), type.name(), source);
    }

    Pair<String, InputStream> getTestAvroFile(BusinessEntity entity, int fileIdx) {
        return getTestAvroFile(entity, null, fileIdx);
    }

    Pair<String, InputStream> getTestAvroFile(BusinessEntity entity, String suffix, int fileIdx) {
        String fileName;
        if (StringUtils.isNotBlank(suffix)) {
            fileName = String.format("%s_%s-%d.avro", entity.name(), suffix, fileIdx);
        } else {
            fileName = String.format("%s-%d.avro", entity.name(), fileIdx);
        }
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, getAvroFileVersion(), fileName);
        return Pair.of(fileName, is);
    }

    void mockCSVImport(BusinessEntity entity, int fileIdx, String feedType) {
        mockCSVImport(entity, null, fileIdx, feedType);
    }

    void mockVISIDBImport(BusinessEntity entity, int fileIdx, String feedType) {
        mockVISIDBImport(entity, null, fileIdx, feedType);
    }

    void mockCSVImport(BusinessEntity entity, String suffix, int fileIdx, String feedType) {
        mockImport(entity, suffix, fileIdx, feedType, SourceType.FILE);
    }

    void mockVISIDBImport(BusinessEntity entity, String suffix, int fileIdx, String feedType) {
        mockImport(entity, suffix, fileIdx, feedType, SourceType.VISIDB);
    }

    void mockImport(BusinessEntity entity, String suffix, int fileIdx, String feedType, SourceType sourceType) {
        List<String> strings = registerMockDataFeedTask(entity, suffix, feedType, sourceType);
        String feedTaskId = strings.get(0);
        String templateName = strings.get(1);
        Date now = new Date();
        Pair<String, InputStream> testAvroArtifact = getTestAvroFile(entity, suffix, fileIdx);
        String fileName = testAvroArtifact.getLeft();
        InputStream is = testAvroArtifact.getRight();
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String extractPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                sourceType.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(now));
        long numRecords;
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, extractPath + "/part-00000.avro");
            numRecords = AvroUtils.count(yarnConfiguration, extractPath + "/*.avro");
            log.info("Uploaded " + numRecords + " records from " + fileName + " to " + extractPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro file " + fileName);
        }
        Extract extract = new Extract();
        extract.setName("Extract-" + templateName);
        extract.setPath(extractPath);
        extract.setProcessedRecords(numRecords);
        extract.setExtractionTimestamp(now.getTime());
        List<String> tableNames = dataFeedProxy.registerExtract(customerSpace.toString(), feedTaskId, templateName,
                extract);
        registerImportAction(feedTaskId, numRecords, tableNames,
                sourceType == SourceType.VISIDB ? CDLConstants.DEFAULT_VISIDB_USER : INITIATOR);
    }

    private Table getMockTemplate(BusinessEntity entity, String suffix, String feedType) {
        String templateFileName;
        if (StringUtils.isNotBlank(suffix)) {
            templateFileName = String.format("%s_%s_%s.json", entity.name(), suffix, feedType);
        } else {
            templateFileName = String.format("%s_%s.json", entity.name(), feedType);
        }
        log.info("templateFileName is {}.", templateFileName);
        InputStream templateIs = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, getAvroFileVersion(),
                templateFileName);
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(templateIs, Table.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + entity.name() + " template from S3.");
        }
    }

    /**
     * Load {@link S3ImportSystem} from test artifact repo and upsert it
     *
     * @param systemName
     *            target system name
     */
    protected void mockImportSystem(@NotNull String systemName) {
        Preconditions.checkNotNull(systemName, "Cannot mock S3ImportSystem with null name");
        S3ImportSystem system = getMockSystem(systemName);
        // update tenant to test tenant
        system.setTenant(mainTestTenant);
        S3ImportSystem currSystem = cdlProxy.getS3ImportSystem(mainCustomerSpace, systemName);
        if (currSystem != null) {
            cdlProxy.updateS3ImportSystem(mainCustomerSpace, system);
        } else {
            cdlProxy.createS3ImportSystem(mainCustomerSpace, system);
        }
    }

    /*
     * Load S3ImportSystem (stored in serialized JSON) from test artifact s3 bucket.
     * Filename format: "System_<SYSTEM_NAME>.json"
     */
    private S3ImportSystem getMockSystem(@NotNull String systemName) {
        String filename = String.format("System_%s.json", systemName);
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, getAvroFileVersion(), filename);
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(is, S3ImportSystem.class);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to read S3ImportSystem(name=%s) from s3. bucket=%s, version=%s", filename,
                            S3_AVRO_DIR, getAvroFileVersion()));
        }
    }

    private List<String> registerMockDataFeedTask(BusinessEntity entity, String suffix, String feedType,
            SourceType sourceType) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String feedTaskId;
        String templateName = NamingUtils.timestamp(entity.name());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), sourceType.getName(),
                feedType, entity.name());
        if (dataFeedTask == null) {
            dataFeedTask = new DataFeedTask();
            Table importTemplate = getMockTemplate(entity, suffix, feedType);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            importTemplate.setName(templateName);
            dataFeedTask.setImportTemplate(importTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity.name());
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(sourceType.getName());
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            feedTaskId = dataFeedTask.getUniqueId();
        } else {
            feedTaskId = dataFeedTask.getUniqueId();
            templateName = dataFeedTask.getImportTemplate().getName();

        }
        return Arrays.asList(feedTaskId, templateName);
    }

    private List<String> registerMockDataFeedTask(BusinessEntity entity, String suffix, String feedType) {
        return registerMockDataFeedTask(entity, suffix, feedType, SourceType.FILE);
    }

    private void registerImportAction(String feedTaskId, long count, List<String> tableNames, String initiator) {
        log.info(String.format("Registering action for dataFeedTask=%s", feedTaskId));
        ImportActionConfiguration configuration = new ImportActionConfiguration();
        configuration.setDataFeedTaskId(feedTaskId);
        configuration.setImportCount(count);
        configuration.setRegisteredTables(tableNames);
        configuration.setMockCompleted(true);
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setActionInitiator(initiator);
        action.setDescription(feedTaskId);
        action.setTrackingPid(null);
        action.setActionConfiguration(configuration);
        actionProxy.createAction(mainCustomerSpace, action);
    }

    void importData(BusinessEntity entity, String s3FileName) {
        importData(entity, s3FileName, null, false, false);
    }

    void importData(BusinessEntity entity, String s3FileName, String systemName) {
        importData(entity, s3FileName, getFeedType(entity.name(), systemName), false, false);
    }

    void importData(BusinessEntity entity, String s3FileName, String feedType, boolean compressed,
            boolean outsizeFlag) {
        importData(entity, s3FileName, feedType, compressed, outsizeFlag, null);
    }

    void importData(BusinessEntity entity, String s3FileName, String feedType, boolean compressed, boolean outsizeFlag,
            String subType) {
        ApplicationId applicationId = importDataWithApplicationId(entity, s3FileName, feedType, compressed, outsizeFlag,
                subType);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        log.info("Importing S3 file " + s3FileName + " for " + entity + " is finished.");
    }

    ApplicationId importDataWithApplicationId(BusinessEntity entity, String s3FileName, String feedType,
            boolean compressed, boolean outsizeFlag, String subType) {
        Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(s3FileName, outsizeFlag), s3FileName);
        log.info("Streaming S3 file " + s3FileName + " as a template file for " + entity);
        String outputFileName = s3FileName;
        if (feedType == null) {
            feedType = getFeedTypeByEntity(entity.name());
        }
        if (s3FileName.endsWith(".gz")) {
            outputFileName = s3FileName.substring(0, s3FileName.length() - 3);
        }
        SourceFile template = fileUploadProxy.uploadFile(outputFileName, compressed, s3FileName, entity.name(),
                csvResource, outsizeFlag);
        FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(), entity.name(),
                SourceType.FILE.getName(), feedType);
        modifyFieldMappings(entity, fieldMappingDocument);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }

        fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument, entity.name(),
                SourceType.FILE.getName(), feedType);
        log.info("Modified field mapping document is saved, start importing ...");
        ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), feedType, template, template,
                INITIATOR, subType);
        return applicationId;
    }

    void importOnlyDataFromS3(BusinessEntity entity, String s3FileName, DataFeedTask task) {
        Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(s3FileName), s3FileName);
        log.info("Streaming S3 file " + s3FileName + " as a import file for " + entity);
        String outputFileName = String.format("file_%d.csv", DateTime.now().getMillis());
        SourceFile dataFile = fileUploadProxy.uploadFile(outputFileName, false, s3FileName, entity.name(), csvResource,
                false);
        ApplicationId applicationId = submitS3ImportOnlyData(mainTestTenant.getId(), task, dataFile, INITIATOR);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        log.info("Importing S3 file " + s3FileName + " for " + entity + " is finished.");
    }

    void importData(BusinessEntity entity, List<String> s3FileName, String feedType, boolean compressed,
            boolean outsizeFlag) {
        List<ApplicationId> applicationIds = new ArrayList<>();
        if (StringUtils.isBlank(feedType)) {
            feedType = entity.name() + "Schema";
        }
        for (String filename : s3FileName) {
            Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(filename, outsizeFlag), filename);
            log.info("Streaming S3 file " + filename + " as a template file for " + entity);
            String outputFileName = filename;
            if (filename.endsWith(".gz"))
                outputFileName = filename.substring(0, filename.length() - 3);
            SourceFile template = fileUploadProxy.uploadFile(outputFileName, compressed, filename, entity.name(),
                    csvResource, outsizeFlag);
            FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(),
                    entity.name(), SourceType.FILE.getName(), feedType);
            modifyFieldMappings(entity, fieldMappingDocument);
            fieldMappingDocument.getFieldMappings().stream().forEach(fieldMapping -> {
                if (fieldMapping.getMappedField() == null) {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
            });

            fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument, entity.name(),
                    SourceType.FILE.getName(), feedType);
            log.info("Modified field mapping document is saved, start importing ...");
            ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), feedType, template,
                    template, INITIATOR, "");
            applicationIds.add(applicationId);
        }
        int count = 0;
        for (ApplicationId applicationId : applicationIds) {
            JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
            Assert.assertEquals(status, JobStatus.COMPLETED);
            log.info("Importing S3 file " + s3FileName.get(count) + " for " + entity + " is finished.");
            count++;
        }
    }

    protected String getAvroFileVersion() {
        return S3_AVRO_VERSION;
    }

    private void modifyFieldMappings(BusinessEntity entity, FieldMappingDocument fieldMappingDocument) {
        modifyMatchIdMappings(fieldMappingDocument.getFieldMappings());
        switch (entity) {
        case Account:
            modifyFieldMappingsForAccount(fieldMappingDocument);
            break;
        case Contact:
            modifyFieldMappingsForContact(fieldMappingDocument);
            break;
        default:
        }
    }

    /*
     * Modify field mapping for match IDs. Fields need to have the following format
     * to be mapped to other system (for unique ID, need to specify its own system
     * name)
     *
     * Format: <PREFIX>_<SystemName>_<Entity>_<map_to_lattice_id> E.g.,
     * LETest_MapTo_DefaultSystem_Account_True will map this column to default
     * system's account ID and also map to global customer account ID
     */
    private void modifyMatchIdMappings(List<FieldMapping> fieldMappings) {
        for (FieldMapping mapping : fieldMappings) {
            // map ID to other system
            if (mapping.getUserField().startsWith(MAP_ID_PREFIX)) {
                String system = mapping.getUserField().substring(MAP_ID_PREFIX.length());
                // parse system name & entity. Format =
                // <SystemName>_<Entity>_<map_to_lattice_id>
                String[] tokens = system.split("_");
                mapping.setSystemName(tokens[0]);
                mapping.setIdType(BusinessEntity.Account.name().equals(tokens[1]) ? FieldMapping.IdType.Account
                        : FieldMapping.IdType.Contact);
                boolean mapToLatticeId = false;
                if (tokens.length == 3 && tokens[2].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    // map to lattice account ID
                    mapToLatticeId = true;
                }
                mapping.setMapToLatticeId(mapToLatticeId);
                log.info("Map user field [{}] to system [{}] for entity [{}]. MapToLatticeId={}",
                        mapping.getUserField(), mapping.getSystemName(), tokens[1], mapToLatticeId);
            }
        }
    }

    private void modifyFieldMappingsForAccount(FieldMappingDocument fieldMappingDocument) {
        setExternalSystem(fieldMappingDocument.getFieldMappings());
        setAccountDateAttributes(fieldMappingDocument.getFieldMappings());
    }

    private void setExternalSystem(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equalsIgnoreCase("SalesforceAccountID")) {
                    fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.CRM);
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
                if (fieldMapping.getUserField().equalsIgnoreCase("SalesforceSandboxAccountID")) {
                    fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.CRM);
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
                if (fieldMapping.getUserField().equalsIgnoreCase("MarketoAccountID")) {
                    fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.ERP);
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
            }
        }
    }

    private void setAccountDateAttributes(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equalsIgnoreCase("Test Date")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("MM/DD/YY");
                    fieldMapping.setTimeFormatString("");
                    fieldMapping.setTimezone("UTC");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Account Custom Field Date Attribute 'Test Date' field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 2")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("YYYY-MM-DD");
                    fieldMapping.setTimeFormatString(null);
                    fieldMapping.setTimezone("America/Los_Angeles");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Account Custom Field Date Attribute 'Test Date 2' field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 3")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("DD.MM.YY");
                    fieldMapping.setTimeFormatString("00:00:00 24H");
                    fieldMapping.setTimezone("Asia/Shanghai");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Account Custom Field Date Attribute 'Test Date 3' field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 4")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("MM/DD/YYYY");
                    fieldMapping.setTimeFormatString("00-00-00 12H");
                    fieldMapping.setTimezone("Asia/Kolkata");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Account Custom Field Date Attribute 'Test Date 4' field mapping.");
                }
            }
        }
    }

    private void modifyFieldMappingsForContact(FieldMappingDocument fieldMappingDocument) {
        setContactDateAttributes(fieldMappingDocument.getFieldMappings());
    }

    private void setContactDateAttributes(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equalsIgnoreCase("Last_Communication_Date")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("DD-MMM-YY");
                    fieldMapping.setTimeFormatString("00 00 00 24H");
                    fieldMapping.setTimezone("Europe/Berlin");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Contact Custom Field Date Attribute 'Last_Communication_Date' field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Renewal_Date")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("YYYY.MMM.DD");
                    fieldMapping.setTimeFormatString(null);
                    fieldMapping.setTimezone(null);
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.info("Setting Contact Custom Field Date Attribute 'Renewal_Date' field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Last Modified Date")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateFormatString("DD/MM/YYYY");
                    fieldMapping.setTimeFormatString("00:00:00 12H");
                    fieldMapping.setTimezone("America/New_York");
                    if (StringUtils.isBlank(fieldMapping.getMappedField())) {
                        fieldMapping.setMappedField("LastModifiedDate");
                    }
                    fieldMapping.setMappedToLatticeField(true);

                    log.info("Setting Contact Lattice Field Date Attribute 'Last Modified Date' field mapping.");
                }
            }
        }
    }

    private ApplicationId submitS3ImportOnlyData(String customerSpace, DataFeedTask dataFeedTask, SourceFile dataFile,
            String email) {
        log.info(String.format("The email of the s3 file upload initiator is %s", email));
        if (dataFeedTask == null || dataFeedTask.getImportTemplate() == null) {
            throw new IllegalArgumentException(
                    String.format("Cannot find DataFeedTask %s or template is null!", dataFeedTask.getUniqueId()));
        }
        CSVImportConfig metaData = generateDataOnlyImportConfig(customerSpace,
                dataFeedTask.getImportTemplate().getName(), dataFile, email);
        return cdlProxy.submitImportJob(customerSpace, dataFeedTask.getUniqueId(), true, metaData);
    }

    private ApplicationId submitImport(String customerSpace, String entity, String feedType,
            SourceFile templateSourceFile, SourceFile dataSourceFile, String email, String subType) {
        String source = SourceType.FILE.getName();
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateSourceFile, dataSourceFile, email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace, SourceType.FILE.getName(), entity, feedType, subType,
                "", metaData);
        log.info("Creating a data feed task for " + entity + " with id " + taskId);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] { entity, source, feedType });
        }
        return cdlProxy.submitImportJob(customerSpace, taskId, metaData);
    }

    private CSVImportConfig generateImportConfig(String customerSpace, SourceFile templateSourceFile,
            SourceFile dataSourceFile, String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        templateSourceFile.setTableName("SourceFile_" + templateSourceFile.getName().replace(".", "_"));
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateSourceFile.getTableName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);
        return csvImportConfig;
    }

    private CSVImportConfig generateDataOnlyImportConfig(String customerSpace, String templateTableName,
            SourceFile dataSourceFile, String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        if (StringUtils.isEmpty(templateTableName)) {
            throw new RuntimeException("Template table name cannot be empty!");
        }
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateTableName);
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);

        return csvImportConfig;
    }

    private InputStream readCSVInputStreamFromS3(String fileName) {
        return readCSVInputStreamFromS3(fileName, false);
    }

    private InputStream readCSVInputStreamFromS3(String fileName, boolean outsizeFlag) {
        if (outsizeFlag) {
            return testArtifactService.readTestArtifactAsStream(LARGE_CSV_DIR, LARGE_CSV_VERSION, fileName);
        }
        return testArtifactService.readTestArtifactAsStream(S3_CSV_DIR, S3_CSV_VERSION, fileName);
    }

    private class MultipartFileResource extends InputStreamResource {

        private String fileName;

        MultipartFileResource(InputStream inputStream, String fileName) {
            super(inputStream);
            this.fileName = fileName;
        }

        @Override
        public String getFilename() {
            return fileName;
        }

        @Override
        public long contentLength() {
            return -1;
        }
    }

    void verifyActionRegistration() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<Action> actions = actionProxy.getActionsByOwnerId(customerSpace.toString(), null);
        // Assert.assertEquals(actions.size(), ++actionsNumber);
    }

    private void createDataFeed() {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(mainTestTenant);
        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(mainTestTenant);

    }

    long countTableRole(TableRoleInCollection role) {
        return checkpointService.countTableRole(role);
    }

    long countTableRole(TableRoleInCollection role, DataCollection.Version version) {
        return checkpointService.countTableRole(role, version);
    }

    long countInRedshift(BusinessEntity entity) {
        return checkpointService.countInRedshift(entity);
    }

    String getTableName(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        return dataCollectionProxy.getTableName(customerSpace.toString(), role);
    }

    void resumeCrossSellCheckpoint(String checkpoint) throws IOException {
        resumeCheckpoint(checkpoint, String.valueOf(S3_RATING_CHECKPOINTS_VERSION));
    }

    void resumeCheckpoint(String checkpoint) throws IOException {
        resumeCheckpoint(checkpoint, String.valueOf(S3_CHECKPOINTS_VERSION));
    }

    void resumeCheckpoint(String checkpoint, String checkpointVersion) throws IOException {
        checkpointAutoService.setMainTestTenant(checkpointService.getMainTestTenant());
        if (checkpointVersion == null) {
            checkpointVersion = String.valueOf(S3_CHECKPOINTS_VERSION);
        }
        checkpointAutoService.resumeCheckpoint(checkpoint, checkpointVersion);
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
    }

    void saveCheckpoint(String checkpointName) throws IOException {
        saveCheckpoint(checkpointName, String.valueOf(S3_CHECKPOINTS_VERSION + 1), false);
    }

    void saveCheckpoint(String checkpointName, String checkpointVersion, boolean autoUpload) throws IOException {
        if (StringUtils.isBlank(checkpointVersion)) {
            checkpointVersion = String.valueOf(S3_CHECKPOINTS_VERSION + 1);
        }
        if (autoUpload) {
            if (CollectionUtils.isNotEmpty(checkpointService.getPrecedingCheckpoints())) {
                checkpointAutoService.setPrecedingCheckpoints(checkpointService.getPrecedingCheckpoints());
            }
            checkpointAutoService.setMainTestTenant(checkpointService.getMainTestTenant());
            checkpointAutoService.saveCheckpoint(checkpointName, checkpointVersion, mainCustomerSpace);
        } else {
            checkpointService.saveCheckpoint(checkpointName, checkpointVersion, mainCustomerSpace);
        }
    }

    private List<Report> retrieveReport(String appId) {
        Job job = testBed.getRestTemplate().getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", deployedHostPort, appId), //
                Job.class);
        assertNotNull(job);
        return job.getReports();
    }

    Map<String, StatsCube> verifyStats(BusinessEntity... entities) {
        return verifyStats(true, entities);
    }

    Map<String, StatsCube> verifyStats(boolean onlyAllowSpecifiedEntities, BusinessEntity... entities) {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubeMap = container.getStatsCubes();
        for (BusinessEntity entity : entities) {
            Assert.assertTrue(cubeMap.containsKey(entity.name()), "Stats should contain a cube for " + entity);
        }

        if (onlyAllowSpecifiedEntities) {
            Set<BusinessEntity> allowed = new HashSet<>(Arrays.asList(entities));
            for (BusinessEntity entity : BusinessEntity.values()) {
                if (!allowed.contains(entity)) {
                    Assert.assertFalse(cubeMap.containsKey(entity.name()),
                            "Stats should not contain a cube for " + entity);
                }
            }
        }
        return cubeMap;
    }

    void verifyDataCollectionStatus(DataCollection.Version version) {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        Assert.assertTrue(dataCollectionStatus.getAccountCount() > 0);
    }

    void verifyProcessAnalyzeReport(String appId, Map<BusinessEntity, Map<String, Object>> expectedReport) {
        List<Report> reports = retrieveReport(appId);
        assertEquals(reports.size(), 1);
        Report report = reports.get(0);
        log.info("PAReport: {}", report.getJson().getPayload());
        verifySystemActionReport(report);
        verifyDecisionReport(report);
        verifyConsolidateSummaryReport(report, expectedReport);
    }

    private void verifyDecisionReport(Report paReport) {
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(paReport.getJson().getPayload());
            ObjectNode decisionNode = (ObjectNode) report.get(ReportPurpose.PROCESS_ANALYZE_DECISIONS_SUMMARY.getKey());
            Assert.assertNotNull(decisionNode);
            for (JsonNode n : decisionNode) {
                Assert.assertNotNull(n);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + paReport.getJson().getPayload(), e);
        }

    }

    private void verifySystemActionReport(Report paReport) {
        Assert.assertNotNull(paReport);
        Assert.assertNotNull(paReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(paReport.getJson().getPayload()));
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(paReport.getJson().getPayload());
            ArrayNode systemActionNode = (ArrayNode) report.get(ReportPurpose.SYSTEM_ACTIONS.getKey());
            Assert.assertNotNull(systemActionNode);
            for (JsonNode n : systemActionNode) {
                Assert.assertNotNull(n);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + paReport.getJson().getPayload(), e);
        }
    }

    private void verifyConsolidateSummaryReport(Report paReport,
            Map<BusinessEntity, Map<String, Object>> expectedReport) {
        Assert.assertNotNull(paReport);
        Assert.assertNotNull(paReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(paReport.getJson().getPayload()));

        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(paReport.getJson().getPayload());
            ObjectNode entitiesSummaryNode = (ObjectNode) report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());

            expectedReport.forEach((entity, entityReport) -> {
                Assert.assertTrue(entitiesSummaryNode.has(entity.name()));
                ObjectNode entityNode = (ObjectNode) entitiesSummaryNode.get(entity.name());
                Assert.assertNotNull(entityNode);
                ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
                Assert.assertNotNull(consolidateSummaryNode);
                ObjectNode entityNumberNode = (ObjectNode) entityNode.get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey());
                if (entity != BusinessEntity.Product) {
                    Assert.assertNotNull(entityNumberNode);
                }

                entityReport.forEach((reportKey, reportValue) -> {
                    String[] keySplits = reportKey.split("_");
                    if (keySplits[0].equals(ReportPurpose.ENTITIES_SUMMARY.getKey())) {
                        Assert.assertTrue(consolidateSummaryNode.has(keySplits[1]));
                        if (reportValue instanceof Long) {
                            Assert.assertEquals(consolidateSummaryNode.get(keySplits[1]).asLong(), reportValue);
                        } else if (reportValue instanceof String) {
                            Assert.assertFalse(consolidateSummaryNode.get(keySplits[1]).isNull());
                        }
                    } else if (keySplits[0].equals(ReportPurpose.ENTITY_STATS_SUMMARY.getKey())) {
                        Assert.assertTrue(entityNumberNode.has(keySplits[1]));
                        Assert.assertEquals(entityNumberNode.get(keySplits[1]).asLong(), reportValue);
                    } else if (keySplits[0].equals(ReportPurpose.ENTITY_MATCH_SUMMARY.getKey())) {
                        ObjectNode entityMatchNode = (ObjectNode) entityNode
                                .get(ReportPurpose.ENTITY_MATCH_SUMMARY.getKey());
                        Assert.assertNotNull(entityMatchNode);
                        Assert.assertTrue(entityMatchNode.has(keySplits[1]));
                        Assert.assertEquals(entityMatchNode.get(keySplits[1]).asLong(), reportValue);
                    }
                });
            });
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + paReport.getJson().getPayload(), e);
        }
    }

    void verifyDataFeedStatus(DataFeed.Status expected) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertNotNull(dataFeed);
        Assert.assertEquals(dataFeed.getStatus(), expected);
    }

    void verifyActiveVersion(DataCollection.Version expected) {
        Assert.assertEquals(dataCollectionProxy.getActiveVersion(mainTestTenant.getId()), expected);
    }

    void createTestSegments() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment1());
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment2());
        MetadataSegment segment1 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_1);
        MetadataSegment segment2 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_2);
        Assert.assertNotNull(segment1);
        Assert.assertNotNull(segment2);
    }

    void createTestSegment1() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment1());
        MetadataSegment segment1 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_1);
        Assert.assertNotNull(segment1);
    }

    void createTestSegment2() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment2());
        MetadataSegment segment2 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_2);
        Assert.assertNotNull(segment2);
    }

    void createTestSegment3() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment3());
        MetadataSegment segment3 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_3);
        Assert.assertNotNull(segment3);
    }

    void createTestSegmentCuratedAttr() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegmentCuratedAttr());
        MetadataSegment segmentCuratedAttr = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_CURATED_ATTR);
        Assert.assertNotNull(segmentCuratedAttr);
    }

    void createModelingSegment() {
        testMetadataSegmentProxy.createOrUpdate(constructTargetSegment());
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_MODELING);
        Assert.assertNotNull(segment);
    }

    void createTestSegmentProductBundle() {
        testMetadataSegmentProxy.createOrUpdate(constructSegmentForProductBundle());
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_PRODUCT_BUNDLE);
        Assert.assertNotNull(segment);
    }

    private MetadataSegment constructTestSegment1() {
        Bucket websiteBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList(".com"));
        BucketRestriction websiteRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), websiteBkt);
        Bucket.Transaction txn = new Bucket.Transaction("GMm4ZQnMOWpN8Gn7MhZLB7SrGmOss", TimeFilter.ever(), null, null,
                false);
        Bucket purchaseBkt = Bucket.txnBkt(txn);
        BucketRestriction purchaseRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Transaction, "AnyName"), purchaseBkt);
        Restriction accountRestriction = Restriction.builder().and(websiteRestriction, purchaseRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("Engineer"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_1);
        segment.setDisplayName("End2End Segment 1");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    private MetadataSegment constructTestSegment2() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.IN_COLLECTION,
                Arrays.asList("CALIFORNIA", "TEXAS", "MICHIGAN", "NEW YORK"));
        BucketRestriction stateRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"),
                stateBkt);
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("SEGMENT_5"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "SpendAnalyticsSegment"), techBkt);
        Restriction accountRestriction = Restriction.builder().or(stateRestriction, techRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_2);
        segment.setDisplayName("End2End Segment 2");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    private MetadataSegment constructTestSegment3() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.IN_COLLECTION,
                Arrays.asList("CALIFORNIA", "TEXAS", "MICHIGAN", "NEW YORK"));
        BucketRestriction stateRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"),
                stateBkt);
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("General Practice"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "SpendAnalyticsSegment"), techBkt);
        Restriction accountRestriction = Restriction.builder().or(stateRestriction, techRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_3);
        segment.setDisplayName("End2End Segment 3");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    private MetadataSegment constructTestSegmentCuratedAttr() {
        Bucket numberOfContactsBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(1));
        BucketRestriction numberOfContactsRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.CuratedAccount, InterfaceName.NumberOfContacts.name()),
                numberOfContactsBkt);

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_CURATED_ATTR);
        segment.setDisplayName("End2End Segment Curated Attributes");
        segment.setDescription("A test segment for CDL end2end tests checking the number of contacts.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(numberOfContactsRestriction));
        return segment;
    }

    MetadataSegment constructTargetSegment() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Collections.singletonList("Delaware"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "State"), stateBkt);
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_MODELING);
        segment.setDisplayName("End2End Segment Modeling");
        segment.setDescription("A test segment for CDL end2end modeling test.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setAccountRestriction(accountRestriction);
        return segment;
    }

    private MetadataSegment constructSegmentForProductBundle() {
        // bundle1 CMT4: Autosampler Vials and Closures
        // bundle2 CMT3: Other Plasticware
        Bucket.Transaction txn1 = new Bucket.Transaction("4VXsZpo1WU5dpAYAdWTiKD9yZA1sd", TimeFilter.ever(), null, null,
                false);
        Bucket purchaseBkt1 = Bucket.txnBkt(txn1);
        BucketRestriction purchaseRestriction1 = new BucketRestriction(
                new AttributeLookup(BusinessEntity.PurchaseHistory, "AM_4VXsZpo1WU5dpAYAdWTiKD9yZA1sd__EVER__HP"),
                purchaseBkt1);
        Bucket.Transaction txn2 = new Bucket.Transaction("1iHa3C9UQFBPknqKCNW3L6WgUAARc4o", TimeFilter.ever(), null,
                null, false);
        Bucket purchaseBkt2 = Bucket.txnBkt(txn2);
        BucketRestriction purchaseRestriction2 = new BucketRestriction(
                new AttributeLookup(BusinessEntity.PurchaseHistory, "AM_1iHa3C9UQFBPknqKCNW3L6WgUAARc4o__EVER__HP"),
                purchaseBkt2);
        Restriction accountRestriction = Restriction.builder().and(purchaseRestriction1, purchaseRestriction2).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_PRODUCT_BUNDLE);
        segment.setDisplayName("End2End Segment For Product Bundle");
        segment.setDescription("A test segment for CDL end2end product bundle.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));

        return segment;
    }

    void verifyTestSegment1Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_1, expectedCounts);
    }

    void verifyTestSegment2Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_2, expectedCounts);
    }

    void verifyTestSegment3Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_3, expectedCounts);
    }

    void verifyTestSegmentCuratedAttrCounts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_CURATED_ATTR, expectedCounts);
    }

    private void verifySegmentCounts(String segmentName, Map<BusinessEntity, Long> expectedCounts) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        expectedCounts.forEach((entity, count) -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            Assert.assertEquals(immutableSegment.getEntityCount(entity), count, JsonUtils.pprint(immutableSegment));
        });
    }

    Map<BusinessEntity, Long> getSegmentCounts(String segmentName, Set<BusinessEntity> entities) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        Map<BusinessEntity, Long> cnts = new HashMap<>();
        entities.forEach(entity -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            cnts.put(entity, immutableSegment.getEntityCount(entity));
        });
        return cnts;
    }

    void verifySegmentCountsNonNegative(String segmentName, Collection<BusinessEntity> entities) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        entities.forEach(entity -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            Assert.assertTrue(immutableSegment.getEntityCount(entity) > 0, JsonUtils.pprint(immutableSegment));
        });
    }

    void verifySegmentCountsIncreased(Map<BusinessEntity, Long> segmentCnts,
            Map<BusinessEntity, Long> segmentCntsUpdated) {
        Assert.assertTrue(MapUtils.isNotEmpty(segmentCnts));
        Assert.assertTrue(MapUtils.isNotEmpty(segmentCntsUpdated));
        Assert.assertEquals(segmentCnts.size(), segmentCntsUpdated.size());
        segmentCnts.forEach((entity, cnt) -> {
            Assert.assertNotNull(cnt);
            Assert.assertNotNull(segmentCntsUpdated.get(entity));
            Assert.assertTrue(cnt.longValue() < segmentCntsUpdated.get(entity).longValue());
        });
    }

    protected MetadataSegment getSegmentByName(String segmentName) {
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(segmentName);
        int retries = 0;
        while (segment == null && retries++ < 3) {
            log.info("Wait for 1 sec to retry getting rating engine.");
            SleepUtils.sleep(1000L);
            segment = testMetadataSegmentProxy.getSegment(segmentName);
        }
        Assert.assertNotNull(segment,
                "Cannot find rating engine " + segmentName + " in tenant " + mainTestTenant.getId());
        log.info(String.format("Get segment %s:\n%s", segmentName, JsonUtils.serialize(segment)));
        return segment;
    }

    @SuppressWarnings("deprecation")
    RatingEngine createRuleBasedRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(constructTestSegment2());
        ratingEngine.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setUpdatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        Assert.assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getLatestIteration(), JsonUtils.pprint(newEngine));

        String modelId = newEngine.getLatestIteration().getId();
        RuleBasedModel model = constructRuleModel(modelId);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), modelId, model);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    void activateRatingEngine(String engineId) {
        activateRatingEngine(engineId, mainTestTenant);
    }

    private RuleBasedModel constructRuleModel(String modelId) {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt("CALIFORNIA");
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"), bktA);
        ratingRule.setRuleForBucket(RatingBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("BOB"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RatingBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setId(modelId);
        return ruleBasedModel;
    }

    RatingEngine constructRatingEngine(RatingEngineType engineType, MetadataSegment targetSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName("CDL End2End " + engineType + " Engine");
        ratingEngine.setTenant(mainTestTenant);
        ratingEngine.setType(engineType);
        ratingEngine.setSegment(targetSegment);
        ratingEngine.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setUpdatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setCreated(new Date());
        return ratingEngine;
    }

    void configureCrossSellModel(AIModel testAIModel, PredictionType predictionType, ModelingStrategy strategy,
            List<String> targetProducts, List<String> trainingProducts) {
        testAIModel.setPredictionType(predictionType);
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(testAIModel);
        config.setModelingStrategy(strategy);
        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> myMap = new HashMap<>();
        if (ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE.equals(strategy)) {
            myMap.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, new ModelingConfigFilter(
                    CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, ComparisonType.PRIOR_ONLY, 1));
        }
        config.setFilters(myMap);
        config.setTargetProducts(targetProducts);
        config.setTrainingProducts(trainingProducts);
    }

    void configureCustomEventModel(AIModel testAIModel, String sourceFileName, CustomEventModelingType type) {
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(testAIModel);
        if (type == CustomEventModelingType.CDL)
            advancedConf.setDataStores(Arrays.asList(CustomEventModelingConfig.DataStore.CDL,
                    CustomEventModelingConfig.DataStore.DataCloud));
        else {
            advancedConf.setDataStores(Arrays.asList(CustomEventModelingConfig.DataStore.CustomFileAttributes,
                    CustomEventModelingConfig.DataStore.DataCloud));
        }
        advancedConf.setCustomEventModelingType(type);
        advancedConf.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        advancedConf.setExcludePublicDomains(false);
        advancedConf.setSourceFileName(sourceFileName);
        advancedConf.setSourceFileDisplayName(sourceFileName);
        advancedConf.setTransformationGroup(null); // TransformationGroup.ALL
    }

    void verifyRatingEngineCount(String engineId, Map<RatingBucketName, Long> expectedCounts) {
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        int retries = 0;
        while (ratingEngine == null && retries++ < 3) {
            log.info("Wait for 1 sec to retry getting rating engine.");
            SleepUtils.sleep(1000L);
            ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        }
        Assert.assertNotNull(ratingEngine,
                "Cannot find rating engine " + engineId + " in tenant " + mainTestTenant.getId());
        System.out.println(JsonUtils.pprint(ratingEngine));
        // Map<String, Long> counts = ratingEngine.getCountsAsMap();
        // Assert.assertTrue(MapUtils.isNotEmpty(counts));
        // expectedCounts.forEach((bkt, count) -> {
        // if (count > 0) {
        // Assert.assertNotNull(counts.get(bkt.getName()),
        // "Cannot find count for bucket " + bkt.getName() + " in rating
        // engine.");
        // Assert.assertEquals(counts.get(bkt.getName()), count, "Rating engine
        // count " + bkt.getName()
        // + " expected " + counts.get(bkt.getName()) + " found " + count);
        // }
        // });
    }

    List<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity) {
        return servingStoreProxy.getDecoratedMetadataFromCache(mainCustomerSpace, entity);
    }

    void verifyAccountFeatures() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, TableRoleInCollection.AccountFeatures);
        Assert.assertNotNull(tableName);
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model);
        String msg = String.format("AccountFeatures has %d columns while AM has %d columns in the Model group.", //
                CollectionUtils.size(cms), CollectionUtils.size(amCols));
        Assert.assertTrue(CollectionUtils.size(cms) > CollectionUtils.size(amCols) + 1, msg);
    }

    void verifyUpdateActions() {
        List<Action> actions = actionProxy.getActions(mainTestTenant.getId());
        log.info(String.format("actions=%s", actions));
        Assert.assertTrue(CollectionUtils.isNotEmpty(actions));
        Assert.assertTrue(actions.stream().allMatch(action -> action.getOwnerId() != null));
    }

    void verifyBatchStore(Map<BusinessEntity, Long> expectedEntityCount) {
        if (MapUtils.isEmpty(expectedEntityCount)) {
            return;
        }
        expectedEntityCount.forEach((key, value) -> log.info("Row count for batch store of {}: {} -> {}",
                key.getBatchStore().name(), countTableRole(key.getBatchStore()), value));
        expectedEntityCount.forEach((key, value) -> //
        Assert.assertEquals(Long.valueOf(countTableRole(key.getBatchStore())), value, key.getBatchStore().name()));
    }

    void verifyServingStore(Map<BusinessEntity, Long> expectedEntityCount) {
        if (MapUtils.isEmpty(expectedEntityCount)) {
            return;
        }
        expectedEntityCount.forEach((key, value) -> log.info("Row count for serving store of {}: {} -> {}",
                key.getServingStore().name(), countTableRole(key.getServingStore()), value));
        expectedEntityCount.forEach((key, value) -> {
            Assert.assertEquals(Long.valueOf(countTableRole(key.getServingStore())), value,
                    key.getServingStore().name());
        });
    }

    void verifyExtraTableRoles(Map<TableRoleInCollection, Long> expectedTableCount) {
        if (MapUtils.isEmpty(expectedTableCount)) {
            return;
        }
        expectedTableCount.forEach((key, value) -> log.info("Row count for table role of {}: {} -> {}", key.name(),
                countTableRole(key), value));
        expectedTableCount.forEach((key, value) -> //
        Assert.assertEquals(Long.valueOf(countTableRole(key)), value, key.name()));
    }

    void verifyRedshift(Map<BusinessEntity, Long> expectedEntityCount) {
        if (MapUtils.isEmpty(expectedEntityCount)) {
            return;
        }
        expectedEntityCount.forEach((key, value) -> log.info("Row count for redshift table of {}: {} -> {}", key,
                countInRedshift(key), value));
        expectedEntityCount.forEach((key, value) -> Assert.assertEquals(Long.valueOf(countInRedshift(key)), value));
    }

    void runCommonPAVerifications() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion.complement());
        verifyDataCollectionStatus(initialVersion.complement());
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");
    }

    private BusinessCalendar createBusinessCalendar() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("JAN-01");
        calendar.setLongerMonth(1);
        calendar.setCreated(new Date());
        calendar.setUpdated(new Date());
        return calendar;
    }

    void setupBusinessCalendar() {
        periodProxy.saveBusinessCalendar(mainTestTenant.getId(), createBusinessCalendar());
    }

    void setupPurchaseHistoryMetrics() {
        List<ActivityMetrics> metrics = ActivityMetricsTestUtils.fakePurchaseMetrics(mainTestTenant);
        activityMetricsProxy.save(mainCustomerSpace, ActivityType.PurchaseHistory, metrics);
    }

    void setDefaultAPSRollupPeriod() {
        String rollupPeriod = ApsRollingPeriod.BUSINESS_QUARTER.getName();
        String podId = CamilleEnvironment.getPodId();
        Path zkPath = PathBuilder.buildCustomerSpaceServicePath(podId, CustomerSpace.parse(mainCustomerSpace), "CDL");
        zkPath = zkPath.append("DefaultAPSRollupPeriod");
        Camille camille = CamilleEnvironment.getCamille();
        try {
            camille.upsert(zkPath, new Document(rollupPeriod), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update DefaultAPSRollupPeriod", e);
        }
        log.info("Updated DefaultAPSRollupPeriod to " + rollupPeriod);
    }

    boolean isLocalEnvironment() {
        return "dev".equals(leEnv);
    }

    void verifyTxnDailyStore(int totalDays, String minDate, String maxDate, //
            double amount, double quantity, double cost) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        Table dailyTable = dataCollectionProxy.getTable(mainCustomerSpace,
                TableRoleInCollection.ConsolidatedDailyTransaction, activeVersion);
        // Verify number of days
        List<String> dailyFiles;
        try {
            dailyFiles = HdfsUtils.getFilesForDir(yarnConfiguration, dailyTable.getExtractsDirectory());
        } catch (IOException e) {
            throw new RuntimeException("Failed to get daily table extracts.", e);
        }
        dailyFiles = dailyFiles.stream().filter(f -> !f.contains("_SUCCESS")).collect(Collectors.toList());
        Assert.assertEquals(dailyFiles.size(), totalDays);
        // Verify max/min day period
        Pair<Integer, Integer> minMaxPeriods = TimeSeriesUtils.getMinMaxPeriod(yarnConfiguration, dailyTable);
        Assert.assertNotNull(minMaxPeriods);
        int minDay = DateTimeUtils.dateToDayPeriod(minDate);
        int maxDay = DateTimeUtils.dateToDayPeriod(maxDate);
        Assert.assertEquals((int) minMaxPeriods.getLeft(), minDay);
        Assert.assertEquals((int) minMaxPeriods.getRight(), maxDay);
        // Verify daily aggregated result
        int dayPeriod = DateTimeUtils.dateToDayPeriod(VERIFY_DAILYTXN_TXNDATE);
        String dailyFileContainingTargetDay = dailyFiles.stream().filter(f -> f.contains(String.valueOf(dayPeriod)))
                .findFirst().orElse(null);
        Assert.assertNotNull(dailyFileContainingTargetDay);
        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, dailyFileContainingTargetDay);
        GenericRecord verifyRecord = null;
        String aidFld = InterfaceName.AccountId.name();
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            if (VERIFY_DAILYTXN_ACCOUNTID.equals(record.get(aidFld).toString())
                    && VERIFY_DAILYTXN_PRODUCTID.equals(record.get(InterfaceName.ProductId.name()).toString())) {
                verifyRecord = record;
                break;
            }
        }
        Assert.assertNotNull(verifyRecord);
        log.info("Verified record: " + verifyRecord.toString());
        Assert.assertEquals(verifyRecord.get(InterfaceName.TotalAmount.name()), amount);
        Assert.assertEquals(verifyRecord.get(InterfaceName.TotalQuantity.name()), quantity);
        Assert.assertEquals(verifyRecord.get(InterfaceName.TotalCost.name()), cost);
    }

    protected String getFeedTypeByEntity(String entity) {
        return getFeedType(entity, "DefaultSystem");
    }

    protected String getFeedType(String entity, String systemName) {
        String feedType = entity + "Schema";
        String splitChart = "_";
        switch (entity) {
        case "Account":
            feedType = systemName + splitChart + EntityType.Accounts.getDefaultFeedTypeName();
            break;
        case "Contact":
            feedType = systemName + splitChart + EntityType.Contacts.getDefaultFeedTypeName();
            break;
        case "Transaction":
            feedType = systemName + splitChart + EntityType.ProductPurchases.getDefaultFeedTypeName();
            break;
        case "Product":
            feedType = systemName + splitChart + EntityType.ProductBundles.getDefaultFeedTypeName();
            break;
        default:
            break;
        }
        return feedType;
    }

    private boolean isEntityMatchEnabled() {
        FeatureFlagValueMap flags = batonService.getFeatureFlags(CustomerSpace.parse(mainTestTenant.getId()));
        return FeatureFlagUtils.isEntityMatchEnabled(flags);
    }

    void runTestWithRetry(List<String> candidateFailingSteps) {
        Random rand = new Random(System.currentTimeMillis());
        String randomStepToFail = candidateFailingSteps.get(rand.nextInt(candidateFailingSteps.size()));
        runTestWithRetry(randomStepToFail, false);
    }

    void runTestWithRetry(List<String> candidateFailingSteps, boolean isFullRematch) {
        Random rand = new Random(System.currentTimeMillis());
        String randomStepToFail = candidateFailingSteps.get(rand.nextInt(candidateFailingSteps.size()));
        runTestWithRetry(randomStepToFail, isFullRematch);
    }

    private void runTestWithRetry(String failingAtStep, boolean isFullRematch) {
        log.info("Testing failing PA at " + failingAtStep + " and retry ");
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setSkipPublishToS3(isLocalEnvironment());
        request.setFullRematch(isFullRematch);
        request.setFullProfile(false);
        runTestWithRetry(request, failingAtStep);
    }

    private ProcessAnalyzeRequest getProcessRequest(Long currentPATimestamp) {
        return getProcessRequest(currentPATimestamp, false);
    }

    private ProcessAnalyzeRequest getProcessRequest(Long currentPATimestamp, boolean fullRematch) {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setSkipPublishToS3(isLocalEnvironment());
        request.setSkipDynamoExport(true);
        request.setFullProfile(false);
        request.setFullRematch(fullRematch);
        request.setCurrentPATimestamp(currentPATimestamp);
        return request;
    }

    void runTestWithRetry(List<String> candidateFailingSteps, Long currentPATimestamp) {
        Random rand = new Random(System.currentTimeMillis());
        String randomStepToFail = candidateFailingSteps.get(rand.nextInt(candidateFailingSteps.size()));
        runTestWithRetry(getProcessRequest(currentPATimestamp), randomStepToFail);
    }

    private void runTestWithRetry(ProcessAnalyzeRequest request, String failingAtStep) {
        FailingStep failingStep = new FailingStep();
        failingStep.setName(failingAtStep);
        request.setFailingStep(failingStep);
        long start = System.currentTimeMillis();
        processAnalyze(request, JobStatus.FAILED);
        long duration1 = System.currentTimeMillis() - start;
        if (!isLocalEnvironment()) {
            wipeOutContractDirInHdfs();
        }
        start = System.currentTimeMillis();
        retryProcessAnalyze();
        long duration2 = System.currentTimeMillis() - start;
        log.info("Duration of first and second PA are: " + duration1 + " and " + duration2 + " respectively.");
    }

    private void wipeOutContractDirInHdfs() {
        String contractPath = PathBuilder //
                .buildContractPath(podId, CustomerSpace.parse(mainCustomerSpace).getContractId()).toString();
        String tablesPath = PathBuilder //
                .buildDataTablePath(podId, CustomerSpace.parse(mainCustomerSpace)).toString();
        try {
            String filePath = tablesPath + "/File";
            String fileBkPath = contractPath + "/FileBackup";
            log.info("Backing up " + filePath);
            HdfsUtils.copyFiles(yarnConfiguration, filePath, fileBkPath);
            log.info("Wiping out " + tablesPath);
            HdfsUtils.rmdir(yarnConfiguration, tablesPath);
            log.info("Resuming " + filePath);
            HdfsUtils.copyFiles(yarnConfiguration, fileBkPath, filePath);
            Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, filePath));
            HdfsUtils.rmdir(yarnConfiguration, fileBkPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to wipe out hdfs dir.", e);
        }
    }

    protected boolean createS3Folder(String systemName, List<EntityType> entityTypes) {
        List<String> allSubFolders = dropBoxProxy.getAllSubFolders(mainTestTenant.getId(), systemName, null, null);
        for (EntityType entityType : entityTypes) {
            String folderName = S3PathBuilder.getFolderName(systemName, entityType.getDefaultFeedTypeName());
            if (!allSubFolders.contains(folderName)) {
                dropBoxProxy.createTemplateFolder(mainTestTenant.getId(), systemName,
                        entityType.getDefaultFeedTypeName(), null);
                log.info("create folder {} success.", folderName);
            }
        }
        return true;
    }

}
