package com.latticeengines.scoringapi.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.latticeengines.common.exposed.rest.DetailedErrors;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.scoringapi.exposed.AccountScoreRequest;
import com.latticeengines.scoringapi.exposed.ContactScoreRequest;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.history.ScoreHistorian;
import com.latticeengines.scoringapi.history.ScoreHistoryEntry;
import com.latticeengines.scoringapi.match.ProprietaryDataMatcher;
import com.latticeengines.scoringapi.model.ModelEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;
import com.latticeengines.scoringapi.transform.RecordTransformer;
import com.latticeengines.scoringapi.unused.CombinationElement;
import com.latticeengines.scoringapi.unused.CombinationRetriever;
import com.latticeengines.scoringapi.unused.ScoreRequest;
import com.latticeengines.scoringapi.unused.ScoreType;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "score", description = "REST resource for interacting with score API")
@RestController
@RequestMapping("")
@DetailedErrors
public class ScoreResource {

    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(@PathVariable ModelType type) {
        return activeModelMap.get(type);
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(@PathVariable String modelId) {
        return modelFields.get(modelId);
    }

    @RequestMapping(value = "/accounts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score an account")
    public ScoreResponse scoreAccount(@RequestBody AccountScoreRequest request) {
        return simulateScore();
    }

    @RequestMapping(value = "/contacts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a contact")
    public ScoreResponse scoreRecord(@RequestBody ContactScoreRequest request) {
        return simulateScore();
    }

    private ScoreResponse simulateScore() {
        ScoreResponse scoreResponse = new ScoreResponse();
        scoreResponse.setScore(ThreadLocalRandom.current().nextInt(5, 99));
        return scoreResponse;
    }

    Map<ModelType, List<Model>> activeModelMap = new HashMap<>();
    Map<String, Fields> modelFields = new HashMap<>();

    @PostConstruct
    public void initializeStubData() throws Exception {
        String modelId1 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9999-PLSModel";
        String modelId2 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9998-PLSModel";
        String modelId3 = "ms__1bcd7c1d-1703-4704-9536-60728bdd9997-PLSModel";

        List<Model> contactModels = new ArrayList<>();
        contactModels.add(new Model(modelId1, "US Contact Model", ModelType.CONTACT));
        contactModels.add(new Model(modelId2, "EU Contact Model", ModelType.CONTACT));

        List<Model> accountModels = new ArrayList<>();
        accountModels.add(new Model("ms__1bcd7c1d-1703-4704-9536-60728bdd9997-PLSModel", "US Account Model",
                ModelType.ACCOUNT));

        activeModelMap.put(ModelType.ACCOUNT, accountModels);
        activeModelMap.put(ModelType.CONTACT, contactModels);

        Fields fields1 = new Fields();
        fields1.setModelId(modelId1);

        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Email", FieldType.STRING));
        fields.add(new Field("Company", FieldType.STRING));
        fields.add(new Field("City", FieldType.STRING));
        fields.add(new Field("State", FieldType.STRING));
        fields.add(new Field("Country", FieldType.STRING));
        fields.add(new Field("CreatedDate", FieldType.LONG));
        fields.add(new Field("LastModifiedDate", FieldType.LONG));
        fields.add(new Field("PostalCode", FieldType.STRING));
        fields.add(new Field("FirstName", FieldType.STRING));
        fields.add(new Field("LastName", FieldType.STRING));
        fields.add(new Field("Title", FieldType.STRING));
        fields.add(new Field("LeadSource", FieldType.STRING));
        fields.add(new Field("Phone", FieldType.STRING));
        fields.add(new Field("AnnualRevenue", FieldType.FLOAT));
        fields.add(new Field("NumberOfEmployees", FieldType.INTEGER));
        fields.add(new Field("Industry", FieldType.STRING));

        fields1.setFields(fields);

        modelFields.put(modelId1, fields1);
        modelFields.put(modelId2, fields1);
        modelFields.put(modelId3, fields1);
    }

    @SuppressWarnings("unused")
    private Map<ScoreType, Object> scoreRecords(@RequestBody ScoreRequest request) {
        try (LogContext context = new LogContext("Space", request.space)) {
            log.info("Received a score request");

            // ScoreHistory entries are written by the ScoreHistorian
            // interceptor to capture malformed requests and deserialization
            // errors and so that the duration field includes serialization
            // and deserialization time.
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder
                    .getRequestAttributes();
            ScoreHistoryEntry history = (ScoreHistoryEntry) attributes.getRequest().getAttribute(
                    ScoreHistorian.ENTRY_KEY);
            history.space = request.space.toString();

            List<CombinationElement> combination = combinationRetriever.getCombination(request.space,
                    request.combination, request.tag);

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

            // TODO Verify only one RECORD_ID field.

            // Verify the combined schema against the incoming record.
            List<String> wrong = new ArrayList<String>();
            for (String name : combined.keySet()) {
                FieldSchema field = combined.get(name);
                if (field.source == FieldSource.REQUEST) {
                    if (!request.record.containsKey(name)) {
                        wrong.add(String.format("%1$s [%2$s] was missing", name, field.type));
                    } else {
                        Object value = request.record.get(name);

                        // Automatically widen integers into longs; Jackson
                        // bases the types on actual width.
                        if (field.type == FieldType.INTEGER && value instanceof Integer) {
                            value = ((Integer) value).longValue();
                            request.record.put(name, value);
                        }

                        if (field.interpretation == FieldInterpretation.RECORD_ID) {
                            history.recordID = value.toString();
                        }

                        if (value != null && !field.type.type().isInstance(value)) {
                            wrong.add(String.format("%1$s [%2$s] was not the correct type", name, field.type));
                        }
                    }
                }
            }

            if (wrong.size() > 0) {
                throw new RuntimeException("Record had missing or invalid fields: " + StringUtils.join(wrong, ", "));
            }

            List<String> extra = new ArrayList<String>();
            for (String name : request.record.keySet()) {
                if (!combined.containsKey(name)) {
                    extra.add(name);
                }
            }

            if (extra.size() > 0) {
                log.info("Record had extra fields: " + StringUtils.join(extra, ", "));
            }

            // Match and join Prop Data.
            Map<String, Object> proprietary = matcher.match(request.space, combined, request.record);
            request.record.putAll(proprietary);

            // TODO Query and join aggregate data.

            // TODO Evaluate the filters to determine the selected model.
            CombinationElement selected = combination.get(0);
            history.modelName = selected.model.name;
            history.modelVersion = selected.model.version;

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            ObjectWriter writer = mapper.writer();
            try {
                history.totality = writer.writeValueAsString(request.record);
            } catch (JsonProcessingException ex) {
                throw new RuntimeException("Failed to serialize data totality", ex);
            }

            // Map<String, Object> transformed =
            // transformer.transform(selected.data.transforms, request.record);

            Map<String, Object> transformed = null;
            ModelEvaluator evaluator = modelRetriever.getEvaluator(request.space, selected.model);

            Map<ScoreType, Object> result = evaluator.evaluate(transformed, selected.derivation);
            result.put(ScoreType.MODEL_NAME, selected.model.name);

            return result;
        }
    }

    @Autowired
    private CombinationRetriever combinationRetriever;

    @Autowired
    private RecordTransformer transformer;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private ProprietaryDataMatcher matcher;

    private static final Log log = LogFactory.getLog(ScoreResource.class);
}