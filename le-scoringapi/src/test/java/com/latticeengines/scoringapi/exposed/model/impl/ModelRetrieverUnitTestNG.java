package com.latticeengines.scoringapi.exposed.model.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

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
import com.google.common.collect.Iterators;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public class ModelRetrieverUnitTestNG {

    private static final String TARGETDIR = "/tmp/modelretrieverunittest/modelfiles";

    private CustomerSpace space;

    private String modelId;

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
        space = CustomerSpace.parse("space");
        modelId = "modelId";
        doReturn(ModelSummaryStatus.INACTIVE).when(newModelSummary).getStatus();
        doReturn(System.currentTimeMillis()).when(newModelSummary).getLastUpdateTime();
        doReturn(modelId).when(newModelSummary).getId();
        doReturn(new Tenant(space.getTenantId())).when(newModelSummary).getTenant();
        doReturn(newModelSummary).when(modelRetriever).getModelSummary(any(CustomerSpace.class), any(String.class));
        doReturn(ModelSummaryStatus.ACTIVE).when(oldModelSummary).getStatus();
        scoringArtifact = new ScoringArtifacts(oldModelSummary, null, null, null, null, null, null, null, null, null);
        doReturn(scoringArtifact).when(modelRetriever).retrieveModelArtifactsFromHdfs(any(CustomerSpace.class),
                any(String.class));
        bucketMetadataList = Collections.emptyList();
        doReturn(Collections.singletonList(newModelSummary)).when(modelRetriever)
                .getModelSummariesModifiedWithinTimeFrame(Matchers.anyLong());
        doReturn(bucketMetadataList).when(modelRetriever).getBucketMetadata(any(CustomerSpace.class),
                any(String.class));
        modelRetriever.setScoreArtifactCacheMaxSize(50);
        modelRetriever.setScoreArtifactCacheExpirationTime(1);
        modelRetriever.setScoreArtifactCacheRefreshTime(1);
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        modelRetriever.setTaskScheduler(taskScheduler);
        taskScheduler.setThreadNamePrefix("poolScheduler");
        taskScheduler.setPoolSize(1);
        taskScheduler.initialize();
        modelRetriever.instantiateCache();
        modelRetriever.scheduleRefreshJob();
    }

    @Test(groups = "unit")
    public void testCacheRefresh() throws InterruptedException {
        LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> cache = modelRetriever
                .getScoreArtifactCache();
        Assert.assertNotNull(cache);
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 0);
        Thread.sleep(1200);
        // The first time when scoring cache tries to refresh, no change is
        // since there is no entry in the cache.
        cache = modelRetriever.getScoreArtifactCache();
        Assert.assertNotNull(cache);
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 0);

        AbstractMap.SimpleEntry<CustomerSpace, String> entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space,
                modelId);
        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 1);
        Assert.assertEquals(scoringArtifacts.getModelSummary().getStatus(), ModelSummaryStatus.ACTIVE);
        Thread.sleep(1200);
        // The second time when scoring cache tries to refresh, an entry already
        // exists
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 1);
        scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(scoringArtifacts.getModelSummary().getStatus(), ModelSummaryStatus.INACTIVE);
        Assert.assertNotNull(scoringArtifacts.getBucketMetadataList());
        Assert.assertEquals(scoringArtifacts.getBucketMetadataList().size(), 0);
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
