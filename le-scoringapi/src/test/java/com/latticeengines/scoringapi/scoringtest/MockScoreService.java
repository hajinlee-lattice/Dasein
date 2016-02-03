package com.latticeengines.scoringapi.scoringtest;

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
import com.latticeengines.domain.exposed.scoringapi.ModelIdentifier;
import com.latticeengines.scoringapi.controller.ScoreResource;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.match.ProprietaryDataMatcher;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;
import com.latticeengines.scoringapi.transform.RecordTransformer;
import com.latticeengines.scoringapi.unused.CombinationElement;
import com.latticeengines.scoringapi.unused.CombinationRetriever;
import com.latticeengines.scoringapi.unused.ScoreType;

public class MockScoreService {
    @InjectMocks
    private final ScoreResource scoreService = new ScoreResource();

    @Mock
    private ModelRetriever modelRetriever;

    @Mock
    private CombinationRetriever combinationRetriever;

    @Spy
    private RecordTransformer recordTransformer;

    @Spy
    private ProprietaryDataMatcher matcher;

    public MockScoreService(final Map<String, String> modelsPMML, final List<CombinationElement> combination) {
        recordTransformer = new RecordTransformer();
        matcher = new ProprietaryDataMatcher();

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
