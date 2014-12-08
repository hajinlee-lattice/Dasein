package com.latticeengines.skald;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.skald.model.FieldInterpretation;
import com.latticeengines.domain.exposed.skald.model.FieldSchema;
import com.latticeengines.domain.exposed.skald.model.FieldSource;
import com.latticeengines.domain.exposed.skald.model.FieldType;

@Service
public class InternalDataMatcher {
    public Map<String, Object> match(Map<String, FieldSchema> fields, Map<String, Object> record) {
        // TODO Inspect the schema and record to find the best way to join with
        // internal data. Right now it's all email based, which makes it easy.

        String email = null;
        List<String> columns = new ArrayList<String>();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            if (entry.getValue().interpretation == FieldInterpretation.EmailAddress
                    && entry.getValue().type == FieldType.STRING) {
                // Cast should be safe because type checking already happened.
                email = (String) record.get(entry.getKey());
            }

            if (entry.getValue().source == FieldSource.Internal) {
                columns.add(entry.getKey());
            }
        }

        if (columns.size() == 0) {
            return record;
        }

        if (email == null) {
            throw new RuntimeException("No email address found in the record");
        }

        return query(email, columns);
    }

    // TODO Replace this with an actual web service call.
    // It should have the same signature as this function.
    private Map<String, Object> query(String email, List<String> columns) {
        Map<String, Object> results = new HashMap<String, Object>();
        for (String column : columns) {
            results.put(column, null);
        }

        return results;
    }
}
