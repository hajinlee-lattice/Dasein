package com.latticeengines.scoringapi.match;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.FieldInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.scoringapi.infrastructure.ScoringProperties;

@Service
public class ProprietaryDataMatcher {
    public Map<String, Object> match(CustomerSpace space, Map<String, FieldSchema> fields, Map<String, Object> record) {
        // TODO Inspect the schema and record to find the best way to join with
        // proprietary data. Right now it's all email based, which makes it
        // easy.

        String email = null;
        List<String> columns = new ArrayList<String>();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            if (entry.getValue().interpretation == FieldInterpretation.EMAIL_ADDRESS
                    && entry.getValue().type == FieldType.STRING) {
                // Cast should be safe because type checking already happened.
                email = (String) record.get(entry.getKey());
            }

            if (entry.getValue().source == FieldSource.PROPRIETARY) {
                columns.add(entry.getKey());
            }
        }

        if (columns.size() == 0) {
            return record;
        }

        if (email == null) {
            throw new RuntimeException("No email address found in the record");
        }

        if (StringUtils.countMatches(email, "@") != 1) {
            throw new RuntimeException("Record contained an invalid email address");
        }

        // Public domain suffixes are insane, so instead pass the full domain
        // along and assume that the matcher simply uses an endswith comparison.
        String domain = email.split("@")[1];

        Map<String, Object> matched = invoke(space, fields, domain);

        // Add default null values for any missing columns.
        for (String name : columns) {
            if (!matched.containsKey(name)) {
                matched.put(name, null);
            }
        }

        return matched;
    }

    private Map<String, Object> invoke(CustomerSpace space, Map<String, FieldSchema> fields, String domain) {
        Map<String, Object> matched = new HashMap<String, Object>();

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        String pattern = "http://%s/ResultsCacheData.aspx?TenantID=%s&Domain=%s";
        URL address;
        try {
            address = new URL(String.format(pattern, properties.getMatcherAddress(), space, domain));
        } catch (MalformedURLException ex) {
            throw new RuntimeException("Failed to generate matcher URL", ex);
        }

        log.info("Requesting proprietary data from " + address.toString());
        ProprietaryDataResult parsed;
        try (InputStreamReader reader = new InputStreamReader(address.openStream(), StandardCharsets.UTF_8)) {
            parsed = mapper.readValue(reader, ProprietaryDataResult.class);
        } catch (IOException ex) {
            throw new RuntimeException("Failed retrieve proprietary data", ex);
        }

        if (!parsed.Success) {
            // TODO Extract error information more completely.
            throw new RuntimeException("Match attempt did not succeed: " + parsed.Errors.get(0).Message);
        }

        for (ProprietaryDataResult.ValueStructure entry : parsed.Result.Values) {
            if (entry.Key != null && entry.Value != null) {
                FieldSchema field = fields.get(entry.Key.Value);
                if (field != null && field.source == FieldSource.PROPRIETARY) {
                    matched.put(entry.Key.Value, FieldType.parse(field.type, entry.Value.Value));
                }
            }
        }

        return matched;
    }

    @Autowired
    private ScoringProperties properties;

    private static final Log log = LogFactory.getLog(ProprietaryDataMatcher.class);
}
