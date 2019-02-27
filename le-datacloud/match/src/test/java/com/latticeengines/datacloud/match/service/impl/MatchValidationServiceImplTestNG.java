package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

public class MatchValidationServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String DEFAULT_DECISION_GRAPH_NAME = "Pokemon";
    private static final String DECISION_GRAPH_IN_DB = "GingerBread";
    private static final String DECISION_GRAPH_NOT_IN_DB = "JellyBean";

    private static final DecisionGraph DEFAULT_GRAPH_INSTANCE = newGraph(0L);
    private static final DecisionGraph GRAPH_INSTANCE_IN_DB = newGraph(1L);


    @Inject
    @InjectMocks
    private MatchValidationServiceImpl matchValidationService;

    @Mock
    private DecisionGraphEntityMgr decisionGraphEntityMgr;

    @BeforeMethod(groups = "functional")
    private void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        FieldUtils.writeField(
                matchValidationService, "defaultDecisionGraph", DEFAULT_DECISION_GRAPH_NAME, true);
    }

    @Test(groups = "functional")
    private void testValidateDecisionGraph() {
        // setup mocks
        Mockito.when(decisionGraphEntityMgr
                .getDecisionGraph(DEFAULT_DECISION_GRAPH_NAME)).thenReturn(DEFAULT_GRAPH_INSTANCE);
        Mockito.when(decisionGraphEntityMgr
                .getDecisionGraph(DECISION_GRAPH_IN_DB)).thenReturn(GRAPH_INSTANCE_IN_DB);
        Mockito.when(decisionGraphEntityMgr
                .getDecisionGraph(DECISION_GRAPH_NOT_IN_DB)).thenReturn(null);

        // graph exists
        matchValidationService.validateDefaultDecisionGraph();
        matchValidationService.validateDecisionGraph(DEFAULT_DECISION_GRAPH_NAME);
        matchValidationService.validateDecisionGraph(DECISION_GRAPH_IN_DB);

        // graph does not exist, should throw illegal argument exception
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> matchValidationService.validateDecisionGraph(DECISION_GRAPH_NOT_IN_DB));
    }

    private static DecisionGraph newGraph(long id) {
        DecisionGraph graph = new DecisionGraph();
        graph.setPid(id);
        return graph;
    }
}
