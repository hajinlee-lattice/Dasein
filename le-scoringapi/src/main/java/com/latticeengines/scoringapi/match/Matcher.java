package com.latticeengines.scoringapi.match;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.InterpretedFields;

public interface Matcher {

    Map<String, Object> matchAndJoin(CustomerSpace space, InterpretedFields interpreted,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record);

}
