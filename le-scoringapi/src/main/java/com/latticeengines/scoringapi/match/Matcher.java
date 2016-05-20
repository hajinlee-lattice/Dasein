package com.latticeengines.scoringapi.match;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.InterpretedFields;

public interface Matcher {

    Map<String, Object> matchAndJoin(CustomerSpace space, InterpretedFields interpreted,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, ModelSummary modelSummary);

    List<Map<String, Object>> matchAndJoin(CustomerSpace space,
            List<SimpleEntry<Map<String, Object>, InterpretedFields>> parsedRecordAndInterpretedFieldsList,
            Map<String, Map<String, FieldSchema>> fieldSchemasMap, List<ModelSummary> modelSummaryList);

}
