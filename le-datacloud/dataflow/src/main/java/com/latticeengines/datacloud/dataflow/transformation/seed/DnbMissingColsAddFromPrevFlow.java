package com.latticeengines.datacloud.dataflow.transformation.seed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.TblDrivenFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.DnBAddMissingColsConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(DnbMissingColsAddFromPrevFlow.DATAFLOW_BEAN_NAME)
public class DnbMissingColsAddFromPrevFlow
        extends TblDrivenFlowBase<DnBAddMissingColsConfig, DnBAddMissingColsConfig.MapFunc> {
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
        DnBAddMissingColsConfig config = getTransformerConfig(parameters);
        Map<String, Node> sourceMap = initiateSourceMap(parameters, config);
        String seedName = config.getSeed();
        if (sourceMap == null) {
            throw new RuntimeException("Invalid configuration");
        }
        Node prevDnbSeed = null;
        prevDnbSeed = sourceMap.get(seedName);
        if (prevDnbSeed == null) {
            throw new RuntimeException("Failed to prepare seed " + seedName);
        }
        Node newDnbSeed = addSource(parameters.getBaseTables().get(1));
        List<FieldMetadata> newSchema = newDnbSeed.getSchema();
        List<FieldMetadata> oldSchema = prevDnbSeed.getSchema();
        List<String> newSchemaFields = new ArrayList<>();
        for (FieldMetadata field : newSchema) {
            newSchemaFields.add(field.getFieldName());
        }
        List<String> missingCols = new ArrayList<String>();
        List<FieldMetadata> missFieldMetaInfo = new ArrayList<>();
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
        newDnbSeed = newDnbSeed.renamePipe("NewDnBCacheSeed");
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
        return newDnbSeed.merge(addNullRecords);
    }

    @Override
    public Class<? extends TblDrivenFuncConfig> getTblDrivenFuncConfigClass() {
        return DnBAddMissingColsConfig.MapFunc.class;
    }

}
