package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.OutputRecord;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.warnings.Warning;
import com.latticeengines.scoringapi.warnings.WarningCode;
import com.latticeengines.scoringapi.warnings.Warnings;

@Component("matcher)")
public class MatcherImpl implements Matcher {

    private static final Log log = LogFactory.getLog(MatcherImpl.class);

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private Warnings warnings;

    private MatchInput buildMatchInput(CustomerSpace space, InterpretedFields interpreted, Map<String, Object> record) {
        MatchInput matchInput = new MatchInput();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getEmailAddress());
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getDomain());
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getWebsite());
        addToKeyMapIfValueExists(keyMap, MatchKey.Name, interpreted.getCompanyName());
        addToKeyMapIfValueExists(keyMap, MatchKey.City, interpreted.getCompanyCity());
        addToKeyMapIfValueExists(keyMap, MatchKey.State, interpreted.getCompanyState());
        addToKeyMapIfValueExists(keyMap, MatchKey.Country, interpreted.getCompanyCountry());
        matchInput.setKeyMap(keyMap);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.Model);
        matchInput.setTenant(new Tenant(space.toString()));
        List<String> fields = new ArrayList<>();
        List<List<Object>> data = new ArrayList<>();
        List<Object> dataRecord = new ArrayList<>();
        data.add(dataRecord);
        for (String key : record.keySet()) {
            Object value = record.get(key);
            fields.add(key);
            dataRecord.add(value);
        }

        matchInput.setFields(fields);
        matchInput.setData(data);

        return matchInput;
    }

    @Override
    public Map<String, Object> matchAndJoin(CustomerSpace space, InterpretedFields interpreted,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        MatchInput matchInput = buildMatchInput(space, interpreted, record);
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput, true);
        if (matchOutput.getResult().isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.NO_MATCH, new String[] {
                    JsonUtils.serialize(matchInput.getKeyMap()), "No result" }));
        } else {
            List<String> matchFieldNames = matchOutput.getOutputFields();
            OutputRecord outputRecord = matchOutput.getResult().get(0);
            String nameLocationStr = "";
            if (outputRecord.getMatchedNameLocation() != null) {
                nameLocationStr = JsonUtils.serialize(outputRecord.getMatchedNameLocation());
            }
            String errorMessages = outputRecord.getErrorMessages() == null ? "" : Joiner.on(",").join(
                    outputRecord.getErrorMessages());

            log.info(String.format(
                    "{ 'isMatched':'%s', 'matchedDomain':'%s', 'matchedNameLocation':'%s', 'matchErrors':'%s' }",
                    outputRecord.isMatched(), Strings.nullToEmpty(outputRecord.getMatchedDomain()), nameLocationStr,
                    errorMessages));

            if (outputRecord.isMatched()) {
                List<Object> matchFieldValues = outputRecord.getOutput();

                if (matchFieldNames.size() != matchFieldValues.size()) {
                    throw new LedpException(LedpCode.LEDP_31005, new String[] { String.valueOf(matchFieldNames.size()),
                            String.valueOf(matchFieldValues.size()) });
                }
                for (int i = 0; i < matchFieldNames.size(); i++) {
                    String fieldName = matchFieldNames.get(i);
                    FieldSchema schema = fieldSchemas.get(fieldName);
                    if (schema != null && schema.source == FieldSource.PROPRIETARY) {
                        Object fieldValue = matchFieldValues.get(i);
                        record.put(fieldName, fieldValue);
                    }
                }
            } else {
                warnings.addWarning(new Warning(WarningCode.NO_MATCH, new String[] {
                        JsonUtils.serialize(matchInput.getKeyMap()),
                        Strings.nullToEmpty(outputRecord.getMatchedDomain()) + nameLocationStr }));
            }
        }

        log.info(JsonUtils.serialize(record));
        return record;
    }

    private void addToKeyMapIfValueExists(Map<MatchKey, List<String>> keyMap, MatchKey matchKey, String value) {
        if (Strings.isNullOrEmpty(value)) {
            return;
        }
        List<String> keyFields = keyMap.get(matchKey);
        if (keyFields == null) {
            keyFields = new ArrayList<>();
            keyMap.put(matchKey, keyFields);
        }
        keyFields.add(value);
    }
}
