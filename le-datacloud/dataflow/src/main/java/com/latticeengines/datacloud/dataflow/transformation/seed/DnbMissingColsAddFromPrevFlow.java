package com.latticeengines.datacloud.dataflow.transformation.seed;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.DnBAddMissingColsConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(DnbMissingColsAddFromPrevFlow.DATAFLOW_BEAN_NAME)
public class DnbMissingColsAddFromPrevFlow extends ConfigurableFlowBase<DnBAddMissingColsConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DnbMissingColsAppendFromPrevFlow";

    public static final String TRANSFORMER_NAME = "DnBMissingColsAddTransformer";

    private static final String RENAME = "RENAME_";

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
        return DnBAddMissingColsConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node prevDnbSeed = addSource(parameters.getBaseTables().get(0));
        Node newDnbSeed = addSource(parameters.getBaseTables().get(1));
        List<FieldMetadata> newSchema = newDnbSeed.getSchema();
        List<FieldMetadata> oldSchema = prevDnbSeed.getSchema();
        List<String> newSchemaFields = new ArrayList<>();
        for (FieldMetadata field : newSchema) {
            newSchemaFields.add(field.getFieldName());
        }
        List<String> missingCols = new ArrayList<String>();
        List<FieldMetadata> missFieldMetaInfo = new ArrayList<>();
        DnBAddMissingColsConfig config = getTransformerConfig(parameters);
        // Find missing columns in current file compared to previous
        for (FieldMetadata column : oldSchema) {
            if (!newSchemaFields.contains(column.getFieldName())) {
                missingCols.add(column.getFieldName());
                missFieldMetaInfo.add(column);
            }
        }
        missingCols.add(config.getDomain());
        missingCols.add(config.getDuns());
        prevDnbSeed = prevDnbSeed.retain(new FieldList(missingCols));
        prevDnbSeed = prevDnbSeed //
                .rename(new FieldList(config.getDomain(), config.getDuns()),
                        new FieldList(RENAME + config.getDomain(), RENAME + config.getDuns()));
        missingCols.remove(config.getDomain());
        missingCols.remove(config.getDuns());
        Node addNullRecords = newDnbSeed //
                .filter(String.format("%s == null && %s == null", config.getDomain(),
                        config.getDuns()), new FieldList(config.getDomain(), config.getDuns()));
        newDnbSeed = newDnbSeed //
                .filter(String.format("%s != null || %s != null", config.getDomain(),
                        config.getDuns()), new FieldList(config.getDomain(), config.getDuns()))
                .join(new FieldList(config.getDomain(), config.getDuns()), prevDnbSeed,
                        new FieldList(RENAME + config.getDomain(), RENAME + config.getDuns()),
                        JoinType.LEFT) //
                .discard(new FieldList(RENAME + config.getDomain(), RENAME + config.getDuns()));
        
        for (FieldMetadata fieldMeta : missFieldMetaInfo) {
            addNullRecords = addNullRecords.addColumnWithFixedValue(fieldMeta.getFieldName(), null,
                    fieldMeta.getJavaType());
        }
        newDnbSeed = newDnbSeed //
                .merge(addNullRecords);
        return newDnbSeed;
    }

}
