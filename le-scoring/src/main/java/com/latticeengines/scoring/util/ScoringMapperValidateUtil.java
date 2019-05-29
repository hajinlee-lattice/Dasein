package com.latticeengines.scoring.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.util.ModelAndRecordInfo.ModelInfo;

public class ScoringMapperValidateUtil {

    private static final Logger log = LoggerFactory.getLogger(ScoringMapperValidateUtil.class);

    public enum MetadataPurpose {
        FEATURE(3), TARGET(4);
        private int value;

        MetadataPurpose(int value) {
            this.value = value;
        }
    }

    public static void validateTransformation(ModelAndRecordInfo modelAndLeadInfo) {
        long totalRecordCount = modelAndLeadInfo.getTotalRecordCount();
        if (totalRecordCount == 0) {
            log.error("The mapper gets zero record.");
            return;
        }
        long totalRecordTransformed = 0;
        Map<String, ModelInfo> modelInfoMap = modelAndLeadInfo.getModelInfoMap();
        Set<String> modelGuidSet = modelInfoMap.keySet();
        for (String modelGuid : modelGuidSet) {
            totalRecordTransformed += modelInfoMap.get(modelGuid).getRecordCount();
        }
        if (totalRecordCount != totalRecordTransformed) {
            throw new LedpException(LedpCode.LEDP_20010, new String[] { String.valueOf(totalRecordCount),
                    String.valueOf(totalRecordTransformed) });
        }
    }

    public static void validateLocalizedFiles(boolean scoringScriptProvided,
            Map<String, URI> models) {

        if (!scoringScriptProvided) {
            throw new LedpException(LedpCode.LEDP_20002);
        }
        // check whether if model(s) is(are) localized
        if (models.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_20020);
        }
    }

    public static void validateDatatype(JsonNode datatype, JsonNode model, String modelGuid) {

        List<String> datatypeFailures = new ArrayList<String>();
        Map<String, List<String>> modelFailures = new HashMap<String, List<String>>();
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
        List<String> msgs = validate(datatype, modelGuid, metadata);
        if (msgs.size() != 0) {
            modelFailures.put(modelGuid, msgs);
        }

        if (!vf.passDatatypeValidation()) {
            log.error("ValidationResult is: " + vf);
            throw new LedpException(LedpCode.LEDP_20001, new String[] { vf.toString() });
        }
    }

    private static List<String> validate(JsonNode datatype, String modelGuid, ArrayNode metadata) {
        List<String> toReturn = new ArrayList<>();
        if (metadata != null) {
            for (JsonNode obj : metadata) {
                String name = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_NAME).asText();
                int purpose = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_PURPOSE).asInt();
                long type = obj.get(ScoringDaemonService.INPUT_COLUMN_METADATA_VALUETYPE).asLong();
                if (purpose != MetadataPurpose.FEATURE.value) {
                    continue;
                }
                if (!datatype.has(name) || datatype.get(name).isNull()) {
                    String msg = String.format("Missing required column: %s ", name);
                    toReturn.add(msg);
                    continue;
                }
                if (datatype.get(name).asLong() != type) {
                    String msg = String.format("Column %s has type %d in %s which does not match with %d from event table. (0 means non-string type and 1 means string type.)", name,
                            type, modelGuid, datatype.get(name).asLong());
                    toReturn.add(msg);
                }
            }
        } else {
            String msg = String.format("%s does not contain %s. ", modelGuid,
                    ScoringDaemonService.INPUT_COLUMN_METADATA);
            toReturn.add(msg);
        }
        return toReturn;
    }
}
