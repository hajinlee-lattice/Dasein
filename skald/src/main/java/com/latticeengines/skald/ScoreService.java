package com.latticeengines.skald;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ScoreService {
    @RequestMapping(value = "ScoreRecord", method = RequestMethod.POST)
    public Map<String, Object> scoreRecord(@RequestBody ScoreRequest request) {
        log.info(String.format("Received a score request for %1$s model %2$s", request.customerID, request.combination));

        List<ModelElement> active = retriever.getModelCombination(request.customerID, request.combination);

        // TODO Verify all the model schemas against input record.

        // TODO Match and join Prop Data.

        // TODO Query and join aggregate data.

        // TODO Evaluate the filters to determine the selected model.
        ModelElement selected = active.get(0);

        // TODO Apply transformations.

        return evaluator.evaluate(selected, request.record);
    }

    @Autowired
    private ModelRetriever retriever;

    @Autowired
    private ModelEvaluator evaluator;

    private static final Log log = LogFactory.getLog(ScoreService.class);
}