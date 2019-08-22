package com.latticeengines.scoringapi.exposed.model.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

@SuppressWarnings("deprecation")
public class ModelRetrieverUnitTestNG {

    private static final String TARGETDIR = "/tmp/modelretrieverunittest/modelfiles";

    private CustomerSpace space1 = CustomerSpace.parse("space1");

    private String modelId1 = "modelId1";

    private CustomerSpace space2 = CustomerSpace.parse("space2");

    private String modelId2 = "modelId2";

    private String modelId3 = "modelId3";

    private String modelId4 = "modelId4";

    private long mockPmmlSize = 4l;

    private long scoreArtifactCacheMaxWeight = 600;

    private int scoreArtifactCachePMMLFileRatio = 60;

    private int scoreArtifactCacheExpirationTime = 1;

    private int scoreArtifactCacheConcurrencyLevel = 1;

    private int scoreArtifactCacheRefreshTime = 1;

    private double maxCacheThreshold = 0.5;

    private long defaultPmmlFileSize = 2l;

    @SuppressWarnings("unused")
    private long largePmmlFileSize = 5l;

    @Mock
    private ModelSummary oldModelSummary;

    @Mock
    private List<BucketMetadata> bucketMetadataList;

    @Mock
    private ModelSummary newModelSummary;

    @Mock
    private ScoringArtifacts scoringArtifact;

    @Spy
    private ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        FileUtils.deleteDirectory(new File(TARGETDIR));
        new File(TARGETDIR).mkdir();

        MockitoAnnotations.initMocks(this);
        mockModelSummaries();
        mockModelRetriever();
    }

    private void mockModelSummaries() {
        doReturn(ModelSummaryStatus.INACTIVE).when(newModelSummary).getStatus();
        doReturn(System.currentTimeMillis()).when(newModelSummary).getLastUpdateTime();
        doReturn(modelId1).when(newModelSummary).getId();
        doReturn(new Tenant(space1.getTenantId())).when(newModelSummary).getTenant();
        doReturn(ModelSummaryStatus.ACTIVE).when(oldModelSummary).getStatus();
    }

    private void mockModelRetriever() {
        scoringArtifact = new ScoringArtifacts(oldModelSummary, null, null, null, null, null, null, null, null, null);
        doReturn(scoringArtifact).when(modelRetriever).retrieveModelArtifactsFromHdfs(any(CustomerSpace.class),
                any(String.class));
        bucketMetadataList = Collections.emptyList();
        doReturn(newModelSummary).when(modelRetriever).getModelSummary(any(CustomerSpace.class), any(String.class));
        doReturn(Collections.singletonList(newModelSummary)).when(modelRetriever)
                .getModelSummariesModifiedWithinTimeFrame(Matchers.anyLong());
        doReturn(bucketMetadataList).when(modelRetriever).getBucketMetadata(any(CustomerSpace.class),
                any(String.class));
        doReturn(mockPmmlSize).when(modelRetriever).getSizeOfPMMLFile(any(CustomerSpace.class),
                any(ModelSummary.class));
        modelRetriever.getScoreArtifactCache().setScoreArtifactCacheMaxWeight(scoreArtifactCacheMaxWeight);
        modelRetriever.getScoreArtifactCache().setScoreArtifactCachePMMLFileRatio(scoreArtifactCachePMMLFileRatio);
        modelRetriever.getScoreArtifactCache()
                .setScoreArtifactCacheConcurrencyLevel(scoreArtifactCacheConcurrencyLevel);
        modelRetriever.getScoreArtifactCache().setScoreArtifactCacheExpirationTime(scoreArtifactCacheExpirationTime);
        modelRetriever.getScoreArtifactCache().setScoreArtifactCacheRefreshTime(scoreArtifactCacheRefreshTime);
        modelRetriever.getScoreArtifactCache().setScoreArtifactCacheMaxCacheThreshold(maxCacheThreshold);
        modelRetriever.getScoreArtifactCache().setScoreArtifactCacheDefaultPmmlFileSize(defaultPmmlFileSize);
        modelRetriever.getScoreArtifactCache().setTaskScheduler(createThreadPool());

        modelRetriever.getModelDetailsCache().setModelDetailsAndFieldsCacheMaxSize(50);
        modelRetriever.getModelDetailsCache().setModelDetailsAndFieldsCacheExpirationTime(1);

        modelRetriever.getModelFieldsCache().setModelDetailsAndFieldsCacheMaxSize(50);
        modelRetriever.getModelFieldsCache().setModelDetailsAndFieldsCacheExpirationTime(1);

        modelRetriever.instantiateCache();
        modelRetriever.getScoreArtifactCache().scheduleRefreshJob();
    }

    private ThreadPoolTaskScheduler createThreadPool() {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix("poolScheduler");
        taskScheduler.setPoolSize(1);
        taskScheduler.initialize();
        return taskScheduler;
    }

    @Test(groups = "unit")
    public void testCacheRefresh() throws InterruptedException {
        LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> cache = modelRetriever
                .getScoreArtifactCache().getCache();
        Assert.assertNotNull(cache);
        Assert.assertEquals(cache.asMap().size(), 0);
        Thread.sleep(1200);
        // The first time when scoring cache tries to refresh, no change is
        // since there is no entry in the cache.
        cache = modelRetriever.getScoreArtifactCache().getCache();
        Assert.assertNotNull(cache);
        Assert.assertEquals(cache.asMap().size(), 0);

        AbstractMap.SimpleEntry<CustomerSpace, String> entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(
                space1, modelId1);
        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(cache.asMap().size(), 1);
        Assert.assertEquals(scoringArtifacts.getModelSummary().getStatus(), ModelSummaryStatus.ACTIVE);
        Thread.sleep(1200);
        // The second time when scoring cache tries to refresh, an entry already
        // exists
        scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(scoringArtifacts.getModelSummary().getStatus(), ModelSummaryStatus.INACTIVE);
        Assert.assertNotNull(scoringArtifacts.getBucketMetadataList());
        Assert.assertEquals(scoringArtifacts.getBucketMetadataList().size(), 0);

        entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space2, modelId2);
        scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(scoringArtifacts.getModelSummary().getStatus(), ModelSummaryStatus.INACTIVE);
        Assert.assertEquals(cache.asMap().size(), 2);

        // Test the removal of the first entry in the cache
        entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space2, modelId3);
        scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(cache.asMap().size(), 2);
        System.out.println("map key set is " + cache.asMap().keySet());
        Assert.assertFalse(
                cache.asMap().keySet().toString().contains(modelId1)
                        && cache.asMap().keySet().toString().contains(modelId2),
                "One of the previous models should be evicted");

        // Test case where exception happens during fetching the pmml file size
        doThrow(new LedpException(LedpCode.LEDP_31000, new String[] { "modelPath" })).when(modelRetriever)
                .getSizeOfPMMLFile(any(CustomerSpace.class), any(ModelSummary.class));
        entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space2, modelId4);
        scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(cache.asMap().size(), 3);
        System.out.println("map key set is " + cache.asMap().keySet());
        Assert.assertFalse(
                cache.asMap().keySet().toString().contains(modelId1)
                        && cache.asMap().keySet().toString().contains(modelId2),
                "One of the previous models should be evicted");

        // TODO going to remove the comment after the behavior of Guava is clear
        // Test case where pmml file size is too large
        // doReturn(largePmmlFileSize).when(modelRetriever).getSizeOfPMMLFile(any(CustomerSpace.class),
        // any(ModelSummary.class));
        // entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space2,
        // modelId1);
        // try {
        // scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(),
        // entry.getValue());
        // Assert.fail("Should have thrown exception as the file size exceeds
        // the threshold");
        // } catch (Exception e) {
        // Assert.assertTrue(e instanceof LedpException);
        // Assert.assertTrue(((LedpException)
        // e).getCode().equals(LedpCode.LEDP_31026));
        // }
    }

    @Test(groups = "unit")
    public void testRemoveDroppedDataScienceFieldEventTableTransforms() throws IOException {
        InputStream eventTableDataCompositionIs = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/scoringapi/model/eventtable-datacomposition.json");
        String eventTableDataCompositionContents = IOUtils.toString(eventTableDataCompositionIs,
                Charset.defaultCharset());
        DataComposition eventTableDataComposition = JsonUtils.deserialize(eventTableDataCompositionContents,
                DataComposition.class);

        InputStream dataScienceDataCompositionIs = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("com/latticeengines/scoringapi/model/datascience-datacomposition.json");
        String dataScienceDataCompositionContents = IOUtils.toString(dataScienceDataCompositionIs,
                Charset.defaultCharset());
        DataComposition dataScienceDataComposition = JsonUtils.deserialize(dataScienceDataCompositionContents,
                DataComposition.class);

        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        Map<String, FieldSchema> mergedFields = modelRetriever.mergeFields(eventTableDataComposition,
                dataScienceDataComposition);

        Assert.assertEquals(mergedFields.size(), 349);
    }

    @Test(groups = "unit")
    public void testExtractFromModelJson() throws Exception {
        String path = ClassLoader
                .getSystemResource("com/latticeengines/scoringapi/model/3MulesoftAllRows20160314_112802/model.json")
                .getPath();

        ModelRetrieverImpl modelRetriever = new ModelRetrieverImpl();
        modelRetriever.extractFromModelJson(path, TARGETDIR);
        Assert.assertFalse(new File(TARGETDIR + "/STPipelineBinary.p").exists());
    }
}
