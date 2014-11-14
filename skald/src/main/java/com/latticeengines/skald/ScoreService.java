package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.skald.model.FieldSchema;
import com.latticeengines.skald.model.FieldSource;

@RestController
public class ScoreService {
    @RequestMapping(value = "ScoreRecord", method = RequestMethod.POST)
    public Map<String, Object> scoreRecord(@RequestBody ScoreRequest request) {
        log.info(String.format("Received a score request for %1$s model combination %2$s", request.customerID,
                request.combination));

        List<ModelElement> combination = retriever.getModelCombination(request.customerID, request.combination);

        // Verify all the model schemas against incoming record.
        List<String> wrong = new ArrayList<String>();
        for (ModelElement element : combination) {
            for (FieldSchema field : element.model.fields) {
                if (field.source == FieldSource.Request) {
                    if (!request.record.containsKey(field.name)) {
                        wrong.add(String.format("%1$s [%2$s] was missing", field.name, field.type));
                    } else {
                        Object value = request.record.get(field.name);
                        if (value != null && !field.type.type().isInstance(value)) {
                            wrong.add(String.format("%1$s [%2$s] was not the correct type", field.name, field.type));
                        }
                    }
                }
            }
        }

        if (wrong.size() > 0) {
            throw new RuntimeException("Record had missing or invalid fields: " + StringUtils.join(wrong, ", "));
        }

        // TODO Match and join Prop Data.

        // TODO Query and join aggregate data.

        // TODO Evaluate the filters to determine the selected model.
        ModelElement selected = combination.get(0);

        // TODO Apply transformations.

        return evaluator.evaluate(selected, request.record);

        // TODO Write record and results to a score history database.
        // TODO Also do this for failures and capture error information.
    }

    @Autowired
    private ModelRetriever retriever;

    @Autowired
    private ModelEvaluator evaluator;

    private static final Log log = LogFactory.getLog(ScoreService.class);
}