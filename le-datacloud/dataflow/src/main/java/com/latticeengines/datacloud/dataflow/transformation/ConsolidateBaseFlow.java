package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddCompositeColumnFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public abstract class ConsolidateBaseFlow<T extends TransformerConfig> extends ConfigurableFlowBase<T> {
    protected static final String COMPOSITE_KEY = InterfaceName.__Composite_Key__.name();
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
        long currentTime = System.currentTimeMillis();
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

    protected String buildNewIdColumn(ConsolidateDataTransformerConfig config, List<Node> sources) {
        List<String> keys = config.getCompositeKeys();

        for (int i = 0; i < sources.size(); i++) {
            Node source = sources.get(i);
            List<String> fieldNames = source.getFieldNames();
            if (fieldNames.contains(COMPOSITE_KEY)) {
                continue;
            }
            source = source.apply(new ConsolidateAddCompositeColumnFuction(keys, COMPOSITE_KEY), new FieldList(keys),
                    new FieldMetadata(COMPOSITE_KEY, String.class));
            sources.set(i, source);
        }
        return COMPOSITE_KEY;
    }
}