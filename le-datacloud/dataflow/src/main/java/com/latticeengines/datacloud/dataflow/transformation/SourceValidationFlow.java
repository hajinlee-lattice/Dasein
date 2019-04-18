package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ValidationReportFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.SourceValidationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ValidationConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("sourceValidationFlow")
public class SourceValidationFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, SourceValidationFlowParameters> {

    private static final String[] reportAttrs = new String[]{"Rule", "Id"};

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(SourceValidationFlowParameters parameters) {

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> reportAttrs = parameters.getReportAttrs();

        if ((reportAttrs == null) || (reportAttrs.size() == 0)) {
            return null;
        }

        List<ValidationConfig> configs = parameters.getRules();

        List<String> fieldNames = source.getFieldNames();
        FieldList sourceAttrList = new FieldList(fieldNames.toArray(new String[fieldNames.size()]));

        List<Node> validationResults = new ArrayList<Node>();
        for (ValidationConfig config : configs) {
            Node report = validate(source, sourceAttrList, reportAttrs, config);
            if (report != null) {
                validationResults.add(report);
            }
        }

        Node merged = mergeResults(validationResults);
        return merged;
    }

    private Node validate(Node source, FieldList sourceAttrList, List<String> reportAttrList, ValidationConfig config) {
        String ruleName = config.getName();
        Node processed = source;
        Node report = null;
        String filter = config.getFilter();
        if (filter != null) {
            processed = processed.filter(filter, sourceAttrList);
            String validator = config.getValidator();
            if (validator != null) {
                report = null;
            } else {
                report = buildReport(processed, ruleName, sourceAttrList, reportAttrList);
            }
        }
        return report;
    }

    private Node buildReport(Node source, String ruleName, FieldList sourceAttrList, List<String> reportAttrList) {
        String[] outputAttrs = new String[reportAttrs.length + reportAttrList.size()];

        List<FieldMetadata> fms = new ArrayList<>();
        int i;
        for (i = 0; i < reportAttrs.length; i++) {
             outputAttrs[i] = reportAttrs[i];
             fms.add(new FieldMetadata(reportAttrs[i], String.class));
        }

        for (String attr : reportAttrList) {
            outputAttrs[i++] = attr;
        }

        ValidationReportFunction reportFunction = new ValidationReportFunction(ruleName, reportAttrs);
        Node report = source.apply(reportFunction, sourceAttrList, fms, new FieldList(outputAttrs));
        return report;
    }

    private Node mergeResults(List<Node> reports) {
        Node merged = null;
        for (Node report : reports) {
            if (merged == null) {
                merged = report;
            } else {
                merged = merged.merge(report);
            }
        }
        return merged;
    }
}
