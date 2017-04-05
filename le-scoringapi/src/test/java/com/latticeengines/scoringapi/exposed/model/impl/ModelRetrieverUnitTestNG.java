package com.latticeengines.scoringapi.exposed.model.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
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
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;

public class ModelRetrieverUnitTestNG {

    private CustomerSpace space;

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
        MockitoAnnotations.initMocks(this);
        space = CustomerSpace.parse("space");
        when(newModelSummary.getStatus()).thenReturn(ModelSummaryStatus.INACTIVE);
        when(newModelSummary.getLastUpdateTime()).thenReturn(System.currentTimeMillis());
        doReturn(newModelSummary).when(modelRetriever).getModelSummary(any(CustomerSpace.class), any(String.class));
        when(oldModelSummary.getStatus()).thenReturn(ModelSummaryStatus.ACTIVE);
        scoringArtifact = new ScoringArtifacts(oldModelSummary, null, null, null, null, null, null, null, null, null);
        doReturn(scoringArtifact).when(modelRetriever).retrieveModelArtifactsFromHdfs(any(CustomerSpace.class),
                any(String.class));
        bucketMetadataList = Collections.emptyList();
        doReturn(bucketMetadataList).when(modelRetriever).getBucketMetadata(any(CustomerSpace.class),
                any(String.class));
        modelRetriever.scoreArtifactCacheMaxSize = 50;
        modelRetriever.scoreArtifactCacheExpirationTime = 1;
        modelRetriever.scoreArtifactCacheRefreshTime = 1;
        modelRetriever.instantiateCache();
    }

    @Test(groups = "unit")
    public void testCacheRefresh() throws InterruptedException {
        LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, ScoringArtifacts> cache = modelRetriever
                .getScoreArtifactCache();
        Assert.assertNotNull(cache);
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 0);
        Assert.assertNotNull(modelRetriever.getBucketMetadata(space, ""));
        Assert.assertNotNull(modelRetriever.retrieveModelArtifactsFromHdfs(space, ""));

        ScoringArtifacts sa = modelRetriever.retrieveModelArtifactsFromHdfs(space, "");
        Assert.assertNotNull(sa.getModelSummary());
        Assert.assertTrue(sa.getModelSummary().getStatus() == ModelSummaryStatus.ACTIVE);
        AbstractMap.SimpleEntry<CustomerSpace, String> entry = new AbstractMap.SimpleEntry<CustomerSpace, String>(space,
                "modelId");
        ScoringArtifacts scoringArtifacts = modelRetriever.getModelArtifacts(entry.getKey(), entry.getValue());
        Assert.assertNotNull(scoringArtifacts);
        Assert.assertEquals(Iterators.size(cache.asMap().entrySet().iterator()), 1);

        Thread.sleep(2000);
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
}
