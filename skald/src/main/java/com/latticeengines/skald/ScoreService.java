package com.latticeengines.skald;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.skald.model.PredictiveModel;
import com.latticeengines.skald.model.ScoreDerivation;

@RestController
public class ScoreService {
    @RequestMapping(value = "ScoreRecord", method = RequestMethod.POST)
    public Map<String, Map<String, Object>> scoreRecord(@RequestBody ScoreRequest request) {
        log.info(String.format("Received a score request for %1$s model %2$s", request.spaceID, request.modelID));

        PredictiveModel model = retriever.getPredictiveModel(request.spaceID, request.modelID);
        ScoreDerivation derivation = retriever.getScoreDerivation(request.spaceID, request.modelID);

        // TODO: Verify model schema against input record.

        // TODO: Match and join Prop Data.

        // TODO: Query and join aggregate data.

        // TODO: Apply transformations.

        Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
        result.put(request.modelID, evaluator.evaluate(model, derivation, request.record));
        return result;
    }

    @Autowired
    private ModelRetriever retriever;

    @Autowired
    private ModelEvaluator evaluator;

    private static final Log log = LogFactory.getLog(ScoreService.class);
}