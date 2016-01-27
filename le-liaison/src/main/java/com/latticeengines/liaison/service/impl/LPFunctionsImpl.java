package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.latticeengines.liaison.exposed.exception.DefinitionException;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.LPFunctions;

@Component
public class LPFunctionsImpl implements LPFunctions {

    private static final Pattern pattern_named_expression = Pattern
            .compile("^SpecLatticeFunction\\((.*), (DataType(.*?), SpecFunctionType(.*?), SpecFunctionSourceType(.*?), SpecDefaultValue(.*?), SpecDescription\\(\"(.*?)\"\\))\\)$");
    private static final Pattern pattern_version = Pattern
            .compile(".*?LatticeFunctionExpressionConstant\\(\\\"PLS (.*?) Template:\\\".*LatticeFunctionExpressionConstant\\(\\\"(.*?)\\\".*?");

    @Override
    public AbstractMap.SimpleImmutableEntry<String, String> getLPTemplateTypeAndVersion(
            ConnectionMgr conn_mgr) {
        String type = "Unknown";
        String version = "Unknown";

        try {
            String spec = conn_mgr.getSpec("Version");

            Matcher c_named_expression = pattern_named_expression.matcher(spec);
            if (!c_named_expression.matches()) {
                throw new DefinitionException(String.format(
                        "Maude definition of Version cannot be interpretted: %s", spec));
            }
            String defn = c_named_expression.group(1);

            Matcher c_version = pattern_version.matcher(defn);
            if (c_version.matches()) {
                type = c_version.group(1);
                version = c_version.group(2);
            }
            else {
                type = "Nonstandard type";
                version = "Nonstandard version";
            }
        }
        catch (IOException|RuntimeException ex) {
            type = "No template type";
            version = "No template version";
        }

        AbstractMap.SimpleImmutableEntry<String, String> result = new AbstractMap.SimpleImmutableEntry<>(type,version);
        return result;
    }

    @Override
    public Boolean addLDCMatch(ConnectionMgr conn_mgr, String source, String lp_template_version)
            throws IOException, RuntimeException {
        return Boolean.FALSE;
    }

    @Override
    public Map<String, String> getLDCWritebackAttributes(ConnectionMgr conn_mgr)
            throws IOException, RuntimeException {
        Map<String, String> result = new HashMap<>();
        result.put("column_name_in_propdata", "column_name_in_customer_system");
        return result;
    }

    @Override
    public Boolean setLDCWritebackAttributes(ConnectionMgr conn_mgr, String source,
            Map<String, String> attributes, String lp_template_version)
            throws IOException, RuntimeException {
        return Boolean.FALSE;
    }

    @Override
    public Boolean setLDCWritebackAttributesDefaultName(ConnectionMgr conn_mgr, String source,
            Set<String> column_names_in_propdata, String lp_template_type,
            String lp_template_version) throws IOException, RuntimeException {
        return Boolean.FALSE;
    }
}
