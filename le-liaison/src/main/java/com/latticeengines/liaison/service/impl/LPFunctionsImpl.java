package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;
import org.w3c.dom.Element;

import com.latticeengines.liaison.exposed.exception.DefinitionException;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.LPFunctions;
import com.latticeengines.liaison.exposed.service.LoadGroupMgr;

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

        final Set<String> matchFieldsNotUsed = new HashSet<String>(Arrays.asList("Name","City","State","Country"));

        LoadGroupMgr lg_mgr = conn_mgr.getLoadGroupMgr();
        String matchName = String.format("PD_Enrichment_%s", source);

        if (!lg_mgr.hasLoadGroup("PropDataMatch_Step1")) {
            return Boolean.FALSE;
        }

        Element pdm1_pdmatches = lg_mgr.getLoadGroupFunctionality("PropDataMatch_Step1", "pdmatches");
        Element enrichment_match = null;
        Element matchToRemove = null;

        for (int i = 0; i < pdm1_pdmatches.getElementsByTagName("pdmatch").getLength(); i++) {
            Element pdmatch = (Element) pdm1_pdmatches.getElementsByTagName("pdmatch").item(i);
            if (pdmatch.getAttribute("n").equals("PD")) {
                enrichment_match = (Element) pdmatch.cloneNode(Boolean.TRUE);
            }
            else if (pdmatch.getAttribute("n").equals(matchName)) {
                matchToRemove = pdmatch;
            }
        }

        if (matchToRemove != null) {
            pdm1_pdmatches.removeChild(matchToRemove);
        }

        if (enrichment_match == null) {
            return Boolean.FALSE;
        }

        enrichment_match.setAttribute("n", matchName);

        Set<Element> scsChildrenToRemove = new HashSet<>();
        Element scs = (Element) enrichment_match.getElementsByTagName("scs").item(0);
        for (int i = 0; i < scs.getElementsByTagName("c").getLength(); i++) {
            Element c = (Element) scs.getElementsByTagName("c").item(i);
            if (matchFieldsNotUsed.contains(c.getAttribute("mcn"))) {
                scsChildrenToRemove.add(c);
            }
        }

        for (Element c : scsChildrenToRemove) {
            scs.removeChild(c);
        }

        Set<Element> luosChildrenToRemove = new HashSet<>();
        Element luos = (Element) enrichment_match.getElementsByTagName("luos").item(0);
        Element luoNew = (Element) luos.getElementsByTagName("luo").item(0).cloneNode(Boolean.TRUE);
        for (int i = 0; i < luos.getElementsByTagName("luo").getLength(); i++) {
            Element luo = (Element) luos.getElementsByTagName("luo").item(i);
            luosChildrenToRemove.add(luo);
        }

        for (Element luo : luosChildrenToRemove) {
            luos.removeChild(luo);
        }

        luoNew.setAttribute("n", source);
        luos.appendChild(luoNew);

        pdm1_pdmatches.appendChild(enrichment_match);
        lg_mgr.setLoadGroupFunctionality("PropDataMatch_Step1", pdm1_pdmatches);

        return Boolean.TRUE;
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
