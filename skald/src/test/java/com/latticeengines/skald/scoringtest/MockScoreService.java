package com.latticeengines.skald.scoringtest;

import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.skald.model.ModelIdentifier;
import com.latticeengines.skald.CombinationElement;
import com.latticeengines.skald.CombinationRetriever;
import com.latticeengines.skald.InternalDataMatcher;
import com.latticeengines.skald.ModelEvaluator;
import com.latticeengines.skald.ModelRetriever;
import com.latticeengines.skald.RecordTransformer;
import com.latticeengines.skald.ScoreService;
import com.latticeengines.skald.exposed.ScoreRequest;
import com.latticeengines.skald.exposed.ScoreType;

public class MockScoreService {
    @InjectMocks
    private final ScoreService scoreService = new ScoreService();

    @Mock
    private ModelRetriever modelRetriever;

    @Mock
    private CombinationRetriever combinationRetriever;

    @Spy
    private RecordTransformer recordTransformer;

    @Spy
    private InternalDataMatcher matcher;

    public MockScoreService(final Map<String, String> modelsPMML, final List<CombinationElement> combination) {
        recordTransformer = new RecordTransformer();
        matcher = new InternalDataMatcher();

        MockitoAnnotations.initMocks(this);

        // When asking for the specified model combination, return the one from
        // the definition
        when(
                combinationRetriever.getCombination(Mockito.any(CustomerSpace.class), Mockito.anyString(),
                        Mockito.anyString())).thenReturn(combination);

        // When asking for a ModelEvaluator, just look up the model in the
        // definition and return
        // a new ModelEvaluator that operates against it.
        when(modelRetriever.getEvaluator(Mockito.any(CustomerSpace.class), Mockito.any(ModelIdentifier.class)))
                .thenAnswer(new Answer<ModelEvaluator>() {
                    @Override
                    public ModelEvaluator answer(InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        ModelIdentifier modelid = (ModelIdentifier) args[1];
                        // look up the model
                        String pmml = modelsPMML.get(modelid.name);
                        if (pmml == null) {
                            throw new IllegalArgumentException("Cold not locate model with id " + modelid);
                        }

                        return new ModelEvaluator(new StringReader(pmml));
                    }
                });
    }

    public Map<ScoreType, Object> score(Map<String, Object> record) {
        ScoreRequest request = new ScoreRequest(new CustomerSpace("Some", "Test", "Customer"), "Test", record);
        return scoreService.scoreRecord(request);
    }
}
