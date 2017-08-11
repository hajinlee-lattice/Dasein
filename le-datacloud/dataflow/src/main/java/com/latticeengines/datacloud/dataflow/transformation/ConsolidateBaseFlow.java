package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public abstract class ConsolidateBaseFlow<T extends TransformerConfig> extends ConfigurableFlowBase<T> {
    public static String CREATE_DATE = "CREATION_DATE";
    public static String UPDATE_DATE = "UPDATE_DATE";

    protected String processIdColumns(TransformationFlowParameters parameters, ConsolidateDataTransformerConfig config,
            List<Node> sources, List<Table> sourceTables, List<String> sourceNames) {
        String srcId = config.getSrcIdField();
        String masterId = config.getMasterIdField();
        if (masterId == null) {
            masterId = TableRoleInCollection.ConsolidatedAccount.getPrimaryKey().name();
        }
        for (int i = 0; i < parameters.getBaseTables().size(); i++) {
            String sourceName = parameters.getBaseTables().get(i);
            Node source = addSource(sourceName);
            List<String> srcFields = source.getFieldNames();
            if (srcFields.contains(srcId) && !srcFields.contains(masterId)) {
                source = source.rename(new FieldList(srcId), new FieldList(masterId));
                source = source.retain(new FieldList(source.getFieldNames()));
            }
            sources.add(source);
            if (sourceTables != null)
                sourceTables.add(getSourceMetadata(sourceName));
            if (sourceNames != null)
                sourceNames.add(sourceName);
        }
        return masterId;
    }

    protected void createTimestampColumns(ConsolidateDataTransformerConfig config, List<Node> sources) {
        long currentTime = System.currentTimeMillis() / 1000;
        for (int i = 0; i < sources.size(); i++) {
            Node source = sources.get(i);
            if (!source.getFieldNames().contains(CREATE_DATE)) {
                source = source.addColumnWithFixedValue(CREATE_DATE, currentTime, Long.class);
            }
            if (!source.getFieldNames().contains(UPDATE_DATE)) {
                source = source.addColumnWithFixedValue(UPDATE_DATE, currentTime, Long.class);
            }
            sources.set(i, source);
        }
    }
}