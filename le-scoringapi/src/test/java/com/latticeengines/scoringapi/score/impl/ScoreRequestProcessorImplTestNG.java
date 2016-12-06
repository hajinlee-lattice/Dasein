package com.latticeengines.scoringapi.score.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.functionalframework.ScoringApiFunctionalTestNGBase;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public class ScoreRequestProcessorImplTestNG extends ScoringApiFunctionalTestNGBase {

    private CustomerSpace space;

    @Autowired
    @Mock
    private ModelRetriever modelRetriever;

    @Mock
    private ScoreRequest request;

    @Autowired
    @Mock
    private RequestInfo requestInfo;

    @Autowired
    @InjectMocks
    private ScoreRequestProcessor scoreRequestProcessor;

    @BeforeClass
    public void setup() {
        MockitoAnnotations.initMocks(this);
        space = CustomerSpace.parse("space");
        when(request.getModelId()).thenReturn("modelId");
        when(request.getRule()).thenReturn("");
        when(request.getSource()).thenReturn("");
        Mockito.doNothing().when(requestInfo).put(any(String.class), any(String.class));
        ModelSummary modelSummary = new ModelSummary();
        modelSummary.setStatus(ModelSummaryStatus.INACTIVE);
        modelSummary.setName("modelName");
        modelSummary.setId("modelId");
        ScoringArtifacts scoringArtifacts = new ScoringArtifacts(modelSummary, null, null, null, null, null, null, null,
                null);
        when(modelRetriever.getModelArtifacts(space, "")).thenReturn(scoringArtifacts);
    }

    @Test(groups = "functional")
    public void testProcess() {
        boolean thrownException = false;
        try {
            scoreRequestProcessor.process(space, request, false, false, false, "requestId");
        } catch (Exception e) {
            thrownException = true;
            Assert.assertTrue(e instanceof LedpException);
            Assert.assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_31114);
        }
        Assert.assertTrue(thrownException, "Should have thrown exception");
    }
}
