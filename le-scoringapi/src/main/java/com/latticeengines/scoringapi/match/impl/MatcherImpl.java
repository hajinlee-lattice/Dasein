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
import com.latticeengines.common.exposed.util.StringUtils;
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
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.propdata.MatchProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.warnings.Warning;
import com.latticeengines.scoringapi.exposed.warnings.WarningCode;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;
import com.latticeengines.scoringapi.match.Matcher;

@Component("matcher)")
public class MatcherImpl implements Matcher {

    public static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";
    private static final Log log = LogFactory.getLog(MatcherImpl.class);

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private Warnings warnings;

    private MatchInput buildMatchInput(CustomerSpace space, InterpretedFields interpreted, Map<String, Object> record) {
        MatchInput matchInput = new MatchInput();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getEmailAddress(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getDomain(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Domain, interpreted.getWebsite(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Name, interpreted.getCompanyName(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.City, interpreted.getCompanyCity(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.State, interpreted.getCompanyState(), record);
        addToKeyMapIfValueExists(keyMap, MatchKey.Country, interpreted.getCompanyCountry(), record);
        matchInput.setKeyMap(keyMap);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.DerivedColumns);
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
        if (log.isDebugEnabled()) {
            log.debug("matchInput:" + JsonUtils.serialize(matchInput));
        }
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput, true);
        if (log.isDebugEnabled()) {
            log.debug("matchOutput:" + JsonUtils.serialize(matchOutput));
        }

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

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "{ 'isMatched':'%s', 'matchedDomain':'%s', 'matchedNameLocation':'%s', 'matchErrors':'%s' }",
                        outputRecord.isMatched(), Strings.nullToEmpty(outputRecord.getMatchedDomain()),
                        nameLocationStr, errorMessages));
            }

            if (outputRecord.isMatched()) {
                mergeMatchedOutput(matchFieldNames, outputRecord, fieldSchemas, record);
            } else {
                handleUnMatchedOutput(matchFieldNames, matchInput, outputRecord, fieldSchemas, record, nameLocationStr);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(JsonUtils.serialize(record));
        }
        return record;
    }

    private void handleUnMatchedOutput(List<String> matchFieldNames, MatchInput matchInput, OutputRecord outputRecord,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record, String nameLocationStr) {
        for (int i = 0; i < matchFieldNames.size(); i++) {
            String fieldName = matchFieldNames.get(i);
            FieldSchema schema = fieldSchemas.get(fieldName);
            if (schema != null && schema.source == FieldSource.PROPRIETARY) {
                record.put(fieldName, null);
            }
        }
        warnings.addWarning(new Warning(WarningCode.NO_MATCH, new String[] {
                JsonUtils.serialize(matchInput.getKeyMap()),
                Strings.nullToEmpty(outputRecord.getMatchedDomain()) + nameLocationStr }));
    }

    private void mergeMatchedOutput(List<String> matchFieldNames, OutputRecord outputRecord,
            Map<String, FieldSchema> fieldSchemas, Map<String, Object> record) {
        List<Object> matchFieldValues = outputRecord.getOutput();

        if (matchFieldNames.size() != matchFieldValues.size()) {
            throw new LedpException(LedpCode.LEDP_31005, new String[] { String.valueOf(matchFieldNames.size()),
                    String.valueOf(matchFieldValues.size()) });
        }

        for (int i = 0; i < matchFieldNames.size(); i++) {
            String fieldName = matchFieldNames.get(i);
            FieldSchema schema = fieldSchemas.get(fieldName);
            if (schema == null || (schema != null && schema.source != FieldSource.REQUEST)) {
                Object fieldValue = null;
                if (schema != null) {
                    fieldValue = FieldType.parse(schema.type, matchFieldValues.get(i));
                } else {
                    fieldValue = matchFieldValues.get(i);
                }
                record.put(fieldName, fieldValue);
                if (fieldName.equals(IS_PUBLIC_DOMAIN)) {
                    Boolean isPublicDomain = (Boolean) fieldValue;
                    if (isPublicDomain) {
                        warnings.addWarning(new Warning(WarningCode.PUBLIC_DOMAIN, new String[] { Strings
                                .nullToEmpty(outputRecord.getMatchedDomain()) }));
                    }
                }
            }
        }
    }

    private void addToKeyMapIfValueExists(Map<MatchKey, List<String>> keyMap, MatchKey matchKey, String field,
            Map<String, Object> record) {
        Object value = record.get(field);

        if (StringUtils.objectIsNullOrEmptyString(value)) {
            return;
        }
        List<String> keyFields = keyMap.get(matchKey);
        if (keyFields == null) {
            keyFields = new ArrayList<>();
            keyMap.put(matchKey, keyFields);
        }
        keyFields.add(field);
    }
}
