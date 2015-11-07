package com.latticeengines.scoring.util;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringMapper;
import com.latticeengines.scoring.util.ModelAndLeadInfo.ModelInfo;

public class ScoringMapperValidateUtil {

    private static final Log log = LogFactory.getLog(EventDataScoringMapper.class);

    static public enum MetadataPurpose {
        FEATURE(3), TARGET(4);
        private int value;

        private MetadataPurpose(int value) {
            this.value = value;
        }
    }

    public static void validateTransformation(ModelAndLeadInfo modelAndLeadInfo) {
        long totalLeadsPasssed = modelAndLeadInfo.getTotalleadNumber();
        long totalLeadsTransformed = 0;
        Map<String, ModelInfo> modelInfoMap = modelAndLeadInfo.getModelInfoMap();
        Set<String> modelGuidSet = modelInfoMap.keySet();
        for (String modelGuid : modelGuidSet) {
            totalLeadsTransformed += modelInfoMap.get(modelGuid).getLeadNumber();
        }
        if (totalLeadsPasssed != totalLeadsTransformed) {
            throw new LedpException(LedpCode.LEDP_20010, new String[] { String.valueOf(totalLeadsPasssed),
                    String.valueOf(totalLeadsTransformed) });
        }
    }

    public static void validateLocalizedFiles(boolean scoringScriptProvided, boolean datatypeFileProvided,
            Map<String, JsonNode> models) {

        if (!scoringScriptProvided) {
            throw new LedpException(LedpCode.LEDP_20002);
        }

        if (!datatypeFileProvided) {
            throw new LedpException(LedpCode.LEDP_20006);
        }

        // check whether if model(s) is(are) localized
        if (models.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20020);
        }
    }

    public static void validateDatatype(JsonNode datatype, JsonNode model, String modelId) {

        List<String> datatypeFailures = new ArrayList<String>();
        Map<String, List<String>> modelFailures = new Hashtable<String, List<String>>();
        DatatypeValidationResult vf = new DatatypeValidationResult(datatypeFailures, modelFailures);

        Iterator<String> keySet = datatype.fieldNames();
        while (keySet.hasNext()) {
            String key = keySet.next();
            long datatypeVal = datatype.get(key).asLong();
            if (datatypeVal != 0 && datatypeVal != 1) {
                String msg = String.format("Column %s contains unknown datatype: %d ", key, datatypeVal);
                datatypeFailures.add(msg);
            }
        }

        // validate the datatype file with the model.json
        ArrayNode metadata = (ArrayNode) model.get(ScoringDaemonService.INPUT_COLUMN_METADATA);
        List<String> msgs = validate(datatype, modelId, metadata);
        if (msgs.size() != 0) {
            modelFailures.put(modelId, msgs);
        }

        if (!vf.passDatatypeValidation()) {
            log.error("ValidationResult is: " + vf);
            throw new LedpException(LedpCode.LEDP_20001, new String[] { vf.toString() });
        }
    }

    private static List<String> validate(JsonNode datatype, String modelID, ArrayNode metadata) {
        List<String> toReturn = new ArrayList<String>();
        if (metadata != null) {
            for (int i = 0; i < metadata.size(); i++) {
                JsonNode obj = metadata.get(i);
                String name = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_NAME).asText();
                Long purpose = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_PURPOSE).asLong();
                long type = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_VALUETYPE).asLong();
                if (purpose.intValue() != MetadataPurpose.FEATURE.value) {
                    continue;
                }
                if (!datatype.has(name)) {
                    String msg = String.format("Missing required column: %s ", name);
                    toReturn.add(msg);
                    continue;
                }
                if (datatype.get(name).asLong() != type) {
                    String msg = String.format("%d does not match with %d ", type, datatype.get(name).asInt());
                    toReturn.add(msg);
                }
            }
        } else {
            String msg = String.format("%s does not contain %s. ", modelID, ScoringDaemonService.INPUT_COLUMN_METADATA);
            toReturn.add(msg);
        }
        return toReturn;
    }
}
