package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.ManualSeedEnrichDunsFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedEnrichDunsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

import cascading.tuple.Fields;

@Component(ManualSeedEnrichDunsFlow.DATAFLOW_BEAN_NAME)
public class ManualSeedEnrichDunsFlow extends ConfigurableFlowBase<ManualSeedEnrichDunsConfig> {

    public final static String DATAFLOW_BEAN_NAME = "ManSeedEnrichDunsFlow";
    public final static String TRANSFORMER_NAME = "ManSeedEnrichDunsTransformer";

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ManualSeedEnrichDunsConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ManualSeedEnrichDunsConfig config = getTransformerConfig(parameters);
        /*
         * 1-manualSeedData : ManualSeedCompanyName, ManualSeedPostalCode,
         * ManualSeedCity, ManualSeedState, ManualSeedCountry
         * 2-manualSeedWithoutZip : removing zipCode field 3 -
         * 3-manualSeedWithoutZipCity : removing zipCode and city fields
         */
        // Set 1
        Node manualSeedData = addSource(parameters.getBaseTables().get(0));
        // Set 2
        Node manualSeedWithoutZip = addSource(parameters.getBaseTables().get(1));
        // Set 3
        Node manualSeedWithoutZipCity = addSource(parameters.getBaseTables().get(2));

        // rename & retain sets 2 and 3
        manualSeedWithoutZip = manualSeedWithoutZip //
                .rename(new FieldList(config.getManSeedDuns(), config.getManSeedId()),
                        new FieldList(renameZip(config.getManSeedDuns()),
                                renameZip(config.getManSeedId())));

        // retain the field names to avoid mismatch in the column names
        manualSeedWithoutZip = manualSeedWithoutZip //
                .retain(new FieldList(renameZip(config.getManSeedDuns()), renameZip(config.getManSeedId())));
        manualSeedWithoutZipCity = manualSeedWithoutZipCity //
                .rename(new FieldList(config.getManSeedDuns(), config.getManSeedId()),
                        new FieldList(renameZipCity(config.getManSeedDuns()),
                                renameZipCity(config.getManSeedId())));

        // retain the field names to avoid mismatch in the column names
        manualSeedWithoutZipCity = manualSeedWithoutZipCity //
                .retain(new FieldList(renameZipCity(config.getManSeedDuns()), renameZipCity(config.getManSeedId())));

        // inner join all the 3 tables based on Id
        Node combinedResultSet = manualSeedData //
                .coGroup(new FieldList(config.getManSeedId()), //
                        Arrays.asList(manualSeedWithoutZip, manualSeedWithoutZipCity), //
                        Arrays.asList(
                                new FieldList(renameZip(config.getManSeedId())),
                                new FieldList(renameZipCity(config.getManSeedId()))),
                        JoinType.INNER);

        ManualSeedEnrichDunsFunction enrichDunsFunction = new ManualSeedEnrichDunsFunction(
                new Fields(combinedResultSet.getFieldNamesArray()), config.getManSeedDuns(),
                renameZip(config.getManSeedDuns()), renameZipCity(config.getManSeedDuns()));

        combinedResultSet = combinedResultSet.apply(enrichDunsFunction,
                new FieldList(combinedResultSet.getFieldNames()), combinedResultSet.getSchema(),
                new FieldList(combinedResultSet.getFieldNames()), Fields.REPLACE);

        // retain manual Seed fields
        combinedResultSet = combinedResultSet //
                .retain(new FieldList(manualSeedData.getFieldNames()));
        return combinedResultSet;
    }

    private String renameZip(String field) {
        return "Zip_" + field;
    }

    private String renameZipCity(String field) {
        return "ZipCity_" + field;
    }

}
