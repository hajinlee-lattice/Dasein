package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.FirmoGraphExistingColumnEnrichmentFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.FirmoGraphNewColumnEnrichmentFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceFirmoGraphEnrichmentTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("sourceFirmoGraphEnrichmentFlow")
public class SourceFirmoGraphEnchrimentFlow extends ConfigurableFlowBase<SourceFirmoGraphEnrichmentTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        SourceFirmoGraphEnrichmentTransformerConfig config = getTransformerConfig(parameters);

        String leftMatchField = config.getLeftMatchField();
        String rightMatchField = config.getRightMatchField();
        List<String> enrichingFields = config.getEnrichingFields();
        List<String> enrichedFields = config.getEnrichedFields();
        validationConfig(leftMatchField, rightMatchField, enrichingFields, enrichedFields, parameters.getBaseTables());

        Node sourceLatticeSeed = addSource(parameters.getBaseTables().get(0));
        List<String> latticeSeedFieldNames = sourceLatticeSeed.getFieldNames();
        Node sourceDnbSeed = addSource(parameters.getBaseTables().get(1));
        List<String> dnbSeedFieldNames = sourceDnbSeed.getFieldNames();
        if (!dnbSeedFieldNames.containsAll(enrichingFields)) {
            throw new RuntimeException("Some DnB enriching fields do not exist!");
        }

        List<String> newFieldNames = getNewFields(enrichedFields, latticeSeedFieldNames);

        Node source = sourceLatticeSeed.leftJoin(leftMatchField, sourceDnbSeed, rightMatchField);

        for (int i = 0; i < enrichedFields.size(); i++) {
            String enrichedField = enrichedFields.get(i);
            String enrichingField = enrichingFields.get(i);
            if (!latticeSeedFieldNames.contains(enrichedField)) {
                source = source.apply(new FirmoGraphNewColumnEnrichmentFunction(leftMatchField, rightMatchField,
                        enrichingField, enrichedField), new FieldList(leftMatchField, rightMatchField, enrichingField),
                        new FieldMetadata(enrichedField, String.class));
            } else {
                source = source.apply(new FirmoGraphExistingColumnEnrichmentFunction(leftMatchField, rightMatchField,
                        enrichingField, enrichedField), new FieldList(leftMatchField, rightMatchField, enrichingField,
                        enrichedField), Arrays.asList(source.getSchema(leftMatchField),
                        source.getSchema(rightMatchField), source.getSchema(enrichingField),
                        source.getSchema(enrichedField)), new FieldList(newFieldNames), Fields.REPLACE);
            }
        }

        if (CollectionUtils.isNotEmpty(config.getGroupFields())) {
            source = source.groupByAndLimit(new FieldList(config.getGroupFields()), 1);
        }

        source = source.retain(new FieldList(newFieldNames));

        if (!config.isKeepInternalColumns()) {
            List<String> retainedFieldNames = resolveFieldNames(source.getFieldNames());
            source = source.retain(new FieldList(retainedFieldNames));
        }
        return source;
    }

    private List<String> resolveFieldNames(List<String> origFieldNames) {
        List<String> newFieldNames = new ArrayList<>();
        for (String origFieldName : origFieldNames) {
            String fieldName = origFieldName.trim().toLowerCase();
            if (fieldName.startsWith("__") || fieldName.equalsIgnoreCase("LatticeAccountId")) {
                continue;
            }
            newFieldNames.add(origFieldName);
        }

        return newFieldNames;
    }

    private List<String> getNewFields(List<String> enrichedFields, List<String> latticeSeedFieldNames) {
        List<String> newFieldNames = new ArrayList<>(latticeSeedFieldNames);
        for (String enrichedField : enrichedFields) {
            if (!newFieldNames.contains(enrichedField)) {
                newFieldNames.add(enrichedField);
            }
        }
        return newFieldNames;
    }

    private void validationConfig(String leftMatchField, String rightMatchField, List<String> enrichingFields,
            List<String> enrichedFields, List<String> sources) {
        if (StringUtils.isBlank(leftMatchField) || StringUtils.isBlank(rightMatchField)) {
            throw new RuntimeException("Match fields can not be blank!");
        }
        if (CollectionUtils.isEmpty(enrichedFields) || CollectionUtils.isEmpty(enrichingFields)) {
            throw new RuntimeException("Enrichment fields can not be empty!");
        }
        if (enrichedFields.size() != enrichingFields.size()) {
            throw new RuntimeException("Enriching and Enriched fields do not match in size!");
        }
        if (CollectionUtils.isEmpty(sources) || sources.size() < 2) {
            throw new RuntimeException("The number of sources should be 2 or more");
        }
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceFirmoGraphEnrichmentTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceFirmoGraphEnrichmentFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceFirmoGraphEnrichmentTransformer";

    }
}
