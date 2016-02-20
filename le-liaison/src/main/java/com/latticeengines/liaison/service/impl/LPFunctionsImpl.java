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
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;

@Component
public class LPFunctionsImpl implements LPFunctions {

    private static final Pattern pattern_named_expression = Pattern.compile(
            "^SpecLatticeFunction\\((.*), (DataType(.*?), SpecFunctionType(.*?), SpecFunctionSourceType(.*?), SpecDefaultValue(.*?), SpecDescription\\(\"(.*?)\"\\))\\)$");
    private static final Pattern pattern_version = Pattern.compile(
            ".*?LatticeFunctionExpressionConstant\\(\\\"PLS (.*?) Template:\\\".*LatticeFunctionExpressionConstant\\(\\\"(.*?)\\\".*?");

    @Override
    public AbstractMap.SimpleImmutableEntry<String, String> getLPTemplateTypeAndVersion(ConnectionMgr conn_mgr) {
        String type = "Unknown";
        String version = "Unknown";

        try {
            String spec = conn_mgr.getSpec("Version");

            Matcher c_named_expression = pattern_named_expression.matcher(spec);
            if (!c_named_expression.matches()) {
                throw new DefinitionException(
                        String.format("Maude definition of Version cannot be interpretted: %s", spec));
            }
            String defn = c_named_expression.group(1);

            Matcher c_version = pattern_version.matcher(defn);
            if (c_version.matches()) {
                type = c_version.group(1);
                version = c_version.group(2);
            } else {
                type = "Nonstandard type";
                version = "Nonstandard version";
            }
        } catch (IOException | RuntimeException ex) {
            type = "No template type";
            version = "No template version";
        }

        AbstractMap.SimpleImmutableEntry<String, String> result = new AbstractMap.SimpleImmutableEntry<>(type, version);
        return result;
    }

    @Override
    public Boolean addLDCMatch(ConnectionMgr conn_mgr, String source, String lp_template_version)
            throws IOException, RuntimeException {

        final Set<String> matchFieldsNotUsed = new HashSet<String>(Arrays.asList("Name", "City", "State", "Country"));

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
            } else if (pdmatch.getAttribute("n").equals(matchName)) {
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
    public Map<String, String> getLDCWritebackAttributes(ConnectionMgr conn_mgr, String source,
            String lp_template_version) throws IOException, RuntimeException {

        Map<String, String> result = new HashMap<>();
        LoadGroupMgr lg_mgr = conn_mgr.getLoadGroupMgr();
        int sourceStrLength = source.length();
        String ptld = "PushLeadsLastScoredToDestination";

        if (lg_mgr.hasLoadGroup(ptld)) {
            Element ptld_targetQueries = lg_mgr.getLoadGroupFunctionality(ptld, "targetQueries");
            Element targetQuery = (Element) ptld_targetQueries.getElementsByTagName("targetQuery").item(0);
            Element fsColumnMappings = (Element) targetQuery.getElementsByTagName("fsColumnMappings").item(0);
            for (int j = 0; j < fsColumnMappings.getElementsByTagName("fsColumnMapping").getLength(); j++) {
                Element fsColumnMapping = (Element) fsColumnMappings.getElementsByTagName("fsColumnMapping").item(j);
                String queryColumnName = fsColumnMapping.getAttribute("queryColumnName");
                String fsColumnName = fsColumnMapping.getAttribute("fsColumnName");
                if (queryColumnName.length() > 4 + sourceStrLength
                        && queryColumnName.substring(0, 4 + sourceStrLength).equals(String.format("LDC_%s", source))) {
                    result.put(queryColumnName.substring(5 + sourceStrLength), fsColumnName);
                }
            }
        }

        return result;
    }

    @Override
    public Boolean setLDCWritebackAttributes(ConnectionMgr conn_mgr, String source, Map<String, String> attributes,
            String lp_template_version) throws IOException, RuntimeException {

        final Set<String> loadGroupsToModify = new HashSet<String>(
                Arrays.asList("PushLeadsInDanteToDestination", "PushLeadsLastScoredToDestination"));
        Boolean result = Boolean.FALSE;

        LoadGroupMgr lg_mgr = conn_mgr.getLoadGroupMgr();
        Set<String> queryNames = new HashSet<>();
        int sourceStrLength = source.length();

        for (String ptld : loadGroupsToModify) {
            if (lg_mgr.hasLoadGroup(ptld)) {
                Element ptld_targetQueries = lg_mgr.getLoadGroupFunctionality(ptld, "targetQueries");
                for (int i = 0; i < ptld_targetQueries.getElementsByTagName("targetQuery").getLength(); i++) {
                    Element targetQuery = (Element) ptld_targetQueries.getElementsByTagName("targetQuery").item(i);
                    if (ptld.equals("PushLeadsLastScoredToDestination")) {
                        queryNames.add(targetQuery.getAttribute("name"));
                    }
                    Element fsColumnMappings = (Element) targetQuery.getElementsByTagName("fsColumnMappings").item(0);
                    Element firstMapping = null;
                    Set<Element> mappingsToRemove = new HashSet<>();
                    for (int j = 0; j < fsColumnMappings.getElementsByTagName("fsColumnMapping").getLength(); j++) {
                        Element fsColumnMapping = (Element) fsColumnMappings.getElementsByTagName("fsColumnMapping")
                                .item(j);
                        if (firstMapping == null) {
                            firstMapping = fsColumnMapping;
                        }
                        if (fsColumnMapping.getAttribute("queryColumnName").length() > 4 + sourceStrLength
                                && fsColumnMapping.getAttribute("queryColumnName").substring(0, 4 + sourceStrLength)
                                        .equals(String.format("LDC_%s", source))) {
                            mappingsToRemove.add(fsColumnMapping);
                        }
                    }
                    for (Element fsColumnMapping : mappingsToRemove) {
                        fsColumnMappings.removeChild(fsColumnMapping);
                    }
                    if (firstMapping != null) {
                        for (Map.Entry<String, String> att : attributes.entrySet()) {
                            String ldcCol = att.getKey();
                            String customerCol = att.getValue();
                            Element fsColumnMapping = (Element) firstMapping.cloneNode(Boolean.TRUE);
                            fsColumnMapping.setAttribute("queryColumnName", String.format("LDC_%s_%s", source, ldcCol));
                            fsColumnMapping.setAttribute("fsColumnName", customerCol);
                            fsColumnMappings.appendChild(fsColumnMapping);
                        }
                    }
                }
                lg_mgr.setLoadGroupFunctionality(ptld, ptld_targetQueries);
            }
        }

        for (String queryName : queryNames) {
            Query q = conn_mgr.getQuery(queryName);
            Set<String> colsToRemove = new HashSet<>();
            for (String colName : q.getColumnNames()) {
                if (colName.length() > 4 + sourceStrLength
                        && colName.substring(0, 4 + sourceStrLength).equals(String.format("LDC_%s", source))) {
                    colsToRemove.add(colName);
                }
            }
            for (String colName : colsToRemove) {
                q.removeColumn(colName);
            }
            for (Map.Entry<String, String> att : attributes.entrySet()) {
                String ldcCol = att.getKey();
                String specstr = String.format(
                        "LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"PD_%s\")), ContainerElementName(\"%s\")))",
                        source, ldcCol);
                Map<String, String> metadata = new HashMap<>();
                QueryColumn qc = new QueryColumnVDBImpl(String.format("LDC_%s_%s", source, ldcCol), specstr, metadata);
                qc.setApprovedUsage("None");
                qc.setTags("External");
                qc.setCategory("Lead Enrichment");
                q.appendColumn(qc);
            }
            conn_mgr.setQuery(q);
            result = Boolean.TRUE;
        }

        return result;
    }

    @Override
    public Boolean setLDCWritebackAttributesDefaultName(ConnectionMgr conn_mgr, String source,
            Set<String> column_names_in_propdata, String lp_template_type, String lp_template_version)
                    throws IOException, RuntimeException {

        Map<String, String> attributes = new HashMap<>();

        for (String ldcCol : column_names_in_propdata) {
            if (lp_template_type.equals("SFDC")) {
                String customerCol = String.format("Lattice_%s__c", fieldNameRestrictLength(ldcCol, 32));
                attributes.put(ldcCol, customerCol);
            } else if (lp_template_type.equals("ELQ")) {
                String customerCol = String.format("C_Lattice_%s1", fieldNameRestrictLength(ldcCol, 32));
                attributes.put(ldcCol, customerCol);
            } else if (lp_template_type.equals("MKTO")) {
                String customerCol = String.format("Lattice_%s", fieldNameRestrictLength(ldcCol, 32));
                attributes.put(ldcCol, customerCol);
            }
        }

        return setLDCWritebackAttributes(conn_mgr, source, attributes, lp_template_version);
    }

    @Override
    public void removeLDCWritebackAttributes(ConnectionMgr conn_mgr, String lp_template_version)
            throws IOException, RuntimeException {

        String source = "";
        Map<String, String> attributes = new HashMap<>();
        setLDCWritebackAttributes(conn_mgr, source, attributes, lp_template_version);
    }

    @Override
    public Map<String, String> getTargetTablesAndDataProviders(ConnectionMgr conn_mgr, String lp_template_version)
            throws IOException, RuntimeException {

        Map<String, String> result = new HashMap<>();
        LoadGroupMgr lg_mgr = conn_mgr.getLoadGroupMgr();
        String ptld = "PushLeadsLastScoredToDestination";

        if (lg_mgr.hasLoadGroup(ptld)) {
            Element ptld_targetQueries = lg_mgr.getLoadGroupFunctionality(ptld, "targetQueries");
            for (int i = 0; i < ptld_targetQueries.getElementsByTagName("targetQuery").getLength(); i++) {
                Element targetQuery = (Element) ptld_targetQueries.getElementsByTagName("targetQuery").item(i);
                result.put(targetQuery.getAttribute("fsTableName"), targetQuery.getAttribute("destDataProvider"));
            }
        }

        return result;
    }

    @Override
    public String fieldNameRestrictLength(String customerCol, int maxLength) {

        int length = customerCol.length();
        if (length <= maxLength) {
            return customerCol;
        }

        final String replacement = "_";
        int nTrailingChars = 10;

        final int nReplacement = replacement.length();

        if (maxLength < nReplacement + 2) {
            return customerCol;
        }

        if (maxLength < nTrailingChars + 4) {
            nTrailingChars = maxLength - 4;
        }

        String leadingChars = customerCol.substring(0, maxLength - nTrailingChars - 3);
        String trailingChars = customerCol.substring(length - nTrailingChars, length);

        return (leadingChars + replacement + trailingChars);
    }
}
