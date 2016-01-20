package com.latticeengines.liaison.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.latticeengines.liaison.exposed.exception.DefinitionException;
import com.latticeengines.liaison.exposed.service.QueryColumn;

public class QueryColumnVDBImpl extends QueryColumn {

    private static final Set<String> mdTypes = new HashSet<String>(Arrays.asList("ApprovedUsage",
            "DisplayName", "Category", "StatisticalType", "Tags", "FundamentalType", "Description",
            "DisplayDiscretizationStrategy"));

    private static final Pattern pattern_sqnf_no_md = Pattern
            .compile("^SpecQueryNamedFunctionExpression\\(ContainerElementName\\(\"(\\w*)\"\\), (LatticeFunction.*)\\)$");
    private static final Pattern pattern_sqnf_no_md_alt = Pattern
            .compile("^SpecQueryNamedFunctionMetadata\\(SpecQueryNamedFunctionExpression\\(ContainerElementName\\(\"(\\w*)\"\\), (LatticeFunction.*)\\), SpecExtractDetails\\(empty\\)\\)$");
    private static final Pattern pattern_sqnf_fcnbndry = Pattern
            .compile("^SpecQueryNamedFunctionEntityFunctionBoundary$");
    private static final Pattern pattern_sqnf_has_md = Pattern
            .compile("^SpecQueryNamedFunctionMetadata\\(SpecQueryNamedFunctionExpression\\(ContainerElementName\\(\"(\\w*)\"\\), (LatticeFunction.*)\\), SpecExtractDetails\\(\\((.*)\\)\\)\\)$");

    private static final String tmpl_sed_single = "^SpecExtractDetail\\(\"%s\", \"(.*?)\"\\)(, SpecExtractDetail\\(.*|$)";
    private static final String tmpl_sed_list = "^SpecExtractDetail\\(\"%s\", StringList\\(.*?\"(.*?)\"\\)\\)(, SpecExtractDetail\\(.*|$)";

    public QueryColumnVDBImpl(String definition) throws DefinitionException {
        super(definition);
    }

    public QueryColumnVDBImpl(QueryColumnVDBImpl other) {
        super(other);
    }

    public void initFromDefinition(String defn) throws DefinitionException {

        String name = "NOT SET";
        String expression = "NOT SET";
        Map<String, String> metadata = new HashMap<>();
        boolean isMatched = Boolean.FALSE;

        if (!isMatched) {
            // Case (1): There is no metadata attached
            Matcher c_no_md = pattern_sqnf_no_md.matcher(defn);
            if (c_no_md.matches()) {
                name = c_no_md.group(1);
                expression = c_no_md.group(2);
                isMatched = Boolean.TRUE;
            }
        }

        if (!isMatched) {
            // Case (2): There is an empty metadata object
            Matcher c_no_md_alt = pattern_sqnf_no_md_alt.matcher(defn);
            if (c_no_md_alt.matches()) {
                name = c_no_md_alt.group(1);
                expression = c_no_md_alt.group(2);
                isMatched = Boolean.TRUE;
            }
        }

        if (!isMatched) {
            // Case (3): The entity-attribute boundary
            Matcher c_fcnbndry = pattern_sqnf_fcnbndry.matcher(defn);
            if (c_fcnbndry.matches()) {
                name = "EntityFunctionBoundary";
                expression = "SpecQueryNamedFunctionEntityFunctionBoundary";
                isMatched = Boolean.TRUE;
            }
        }

        if (!isMatched) {
            // Case (4): There is metadata attached
            Matcher c_has_md = pattern_sqnf_has_md.matcher(defn);
            if (c_has_md.matches()) {
                name = c_has_md.group(1);
                expression = c_has_md.group(2);
                isMatched = Boolean.TRUE;

                String sed = c_has_md.group(3);

                while (Boolean.TRUE) {
                    boolean isExtracted = Boolean.FALSE;
                    for (String mdType : mdTypes) {
                        Matcher md_found = Pattern.compile(String.format(tmpl_sed_single, mdType))
                                .matcher(sed);
                        if (md_found.matches()) {
                            metadata.put(mdType, md_found.group(1).replace("\\\"", "\""));
                            sed = md_found.group(2);
                            isExtracted = Boolean.TRUE;
                            break;
                        }
                        md_found = Pattern.compile(String.format(tmpl_sed_list, mdType)).matcher(
                                sed);
                        if (md_found.matches()) {
                            metadata.put(mdType, md_found.group(1).replace("\\\"", "\""));
                            sed = md_found.group(2);
                            isExtracted = Boolean.TRUE;
                            break;
                        }
                    }

                    if (!isExtracted) {
                        throw new DefinitionException(String.format(
                                "Maude definition (SpecExtractDetail) cannot be interpretted: %s",
                                sed));
                    }

                    if (sed.equals("")) {
                        break;
                    }
                    sed = sed.substring(2);
                }
            }
        }

        if (expression.equals("NOT SET")) {
            throw new DefinitionException(String.format(
                    "Maude definition (SpecQueryNamedFunction) cannot be interpretted: %s", defn));
        }

        initFromValues(name, expression, metadata);
    }

    public String definition() {

        if (getName().equals("EntityFunctionBoundary")) {
            return "SpecQueryNamedFunctionEntityFunctionBoundary";
        }

        String mdsep = "";
        StringBuilder mddefn = new StringBuilder(1000);
        Map<String, String> metadata = getMetadata();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            mddefn.append(mdsep).append(
                    String.format("SpecExtractDetail(\"%s\", \"%s\")", entry.getKey(), entry
                            .getValue().replace("\"", "\\\"")));
            mdsep = ", ";
        }

        StringBuilder sqnf = new StringBuilder(1000);
        sqnf.append("SpecQueryNamedFunctionExpression(");
        sqnf.append(String.format("ContainerElementName(\"%s\")", getName()));
        sqnf.append(", " + getExpression());
        sqnf.append(")");

        if (mddefn.toString().equals("")) {
            return sqnf.toString();
        }

        StringBuilder sqnfm = new StringBuilder(1000);
        sqnfm.append("SpecQueryNamedFunctionMetadata(");
        sqnfm.append(sqnf.toString());
        sqnfm.append(", SpecExtractDetails((");
        sqnfm.append(mddefn.toString());
        sqnfm.append("))");
        sqnfm.append(")");

        return sqnfm.toString();
    }
}
