package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.HashMap;
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

import com.latticeengines.domain.exposed.skald.model.FieldSchema;
import com.latticeengines.domain.exposed.skald.model.FieldSource;
import com.latticeengines.skald.exposed.ScoreRequest;
import com.latticeengines.skald.exposed.ScoreType;

@RestController
public class ScoreService {
    @RequestMapping(value = "ScoreRecord", method = RequestMethod.POST)
    public Map<ScoreType, Object> scoreRecord(@RequestBody ScoreRequest request) {
        log.info(String
                .format("Received a score request for %1$s combination %2$s", request.space, request.combination));

        List<CombinationElement> combination = combinationRetriever.getCombination(request.space, request.combination,
                request.tag);

        // Create a combined schema for all models in the combination. This
        // requires that all models in a combination have compatible field
        // schema -- meaning that when they have fields with the same name,
        // those fields have identical schema.
        Map<String, FieldSchema> combined = new HashMap<String, FieldSchema>();
        for (CombinationElement element : combination) {
            for (Map.Entry<String, FieldSchema> entry : element.data.fields.entrySet()) {
                if (combined.containsKey(entry.getKey())) {
                    if (!entry.getValue().equals(combined.get(entry.getKey()))) {
                        throw new RuntimeException(String.format(
                                "Model combination %s has elements with incompatible schemas for field %s",
                                request.combination, entry.getKey()));
                    }
                } else {
                    combined.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Verify the combined schema against the incoming record.
        List<String> wrong = new ArrayList<String>();
        for (String name : combined.keySet()) {
            FieldSchema field = combined.get(name);
            if (field.source == FieldSource.Request) {
                if (!request.record.containsKey(name)) {
                    wrong.add(String.format("%1$s [%2$s] was missing", name, field.type));
                } else {
                    Object value = request.record.get(name);
                    if (value != null && !field.type.type().isInstance(value)) {
                        wrong.add(String.format("%1$s [%2$s] was not the correct type", name, field.type));
                    }
                }
            }
        }

        if (wrong.size() > 0) {
            throw new RuntimeException("Record had missing or invalid fields: " + StringUtils.join(wrong, ", "));
        }

        // Match and join Prop Data.
        Map<String, Object> internal = matcher.match(combined, request.record);
        request.record.putAll(internal);

        // TODO Query and join aggregate data.

        // TODO Evaluate the filters to determine the selected model.
        CombinationElement selected = combination.get(0);

        Map<String, Object> transformed = transformer.transform(selected.data.transforms, request.record);

        ModelEvaluator evaluator = modelRetriever.getEvaluator(request.space, selected.model);

        return evaluator.evaluate(transformed, selected.derivation);

        // TODO Write record and results to a score history database.
        // TODO Also do this for failures and capture error information.
    }

    @Autowired
    private CombinationRetriever combinationRetriever;

    @Autowired
    private RecordTransformer transformer;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private InternalDataMatcher matcher;

    private static final Log log = LogFactory.getLog(ScoreService.class);
}