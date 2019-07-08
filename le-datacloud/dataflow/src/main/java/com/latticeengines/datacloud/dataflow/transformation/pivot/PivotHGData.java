package com.latticeengines.datacloud.dataflow.transformation.pivot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.datacloud.dataflow.transformation.pivot.utils.PivotUtils;
import com.latticeengines.datacloud.dataflow.utils.FlowUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.HGDataBothBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.HGDataNewTechBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PivotTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Buffer;

@Component(PivotHGData.BEAN_NAME)
public class PivotHGData extends ConfigurableFlowBase<PivotTransformerConfig> {
    public static final String BEAN_NAME = "pivotHGData";

    private String rowIdField = "RowId" + UUID.randomUUID().toString().replace("-", "");

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private String domainField;

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return "pivotTransformer";
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PivotTransformerConfig config = getTransformerConfig(parameters);
        domainField = config.getJoinFields()[0];

        Node source = addSource(parameters.getBaseTables().get(0));
        Map<String, Node> sourceMap = new HashMap<>();
        sourceMap.put(parameters.getBaseTables().get(0), source);

        Node last4Month = source.filter("Last_Verified_Date + " + ONE_MONTH * 4 + "L >= LE_Last_Upload_Date",
                new FieldList("Last_Verified_Date", "LE_Last_Upload_Date"));
        last4Month = last4Month.renamePipe("four-month");
        sourceMap.put(parameters.getBaseTables().get(0) + "_LastFourMonth", last4Month);

        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(domainField, String.class));
        fms.add(new FieldMetadata("Both", String.class));
        Buffer<?> buffer = new HGDataBothBuffer(domainField);
        Node both = source.groupByAndBuffer(new FieldList(domainField), buffer, fms);
        both = both.renamePipe("both");
        sourceMap.put(parameters.getBaseTables().get(0) + "_Both", both);

        Node join = PivotUtils.joinedConfigurablePipes(parameters.getColumns(), config.getJoinFields(), sourceMap, rowIdField);
        Node newTech = aggregateNewTechs(last4Month, parameters.getColumns());

        FieldList joinFieldList = new FieldList(config.getJoinFields());
        join = join.join(joinFieldList, newTech, joinFieldList, JoinType.OUTER);
        join = join.addTimestamp(parameters.getTimestampField());
        join = FlowUtils.truncateStringFields(join, parameters.getColumns());
        return PivotUtils.finalRetain(join, parameters.getColumns());
    }

    private FieldMetadata newTechFieldMetadata(List<SourceColumn> columns) {
        for (SourceColumn column : columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.HGDATA_NEWTECH)) {
                return new FieldMetadata(column.getColumnName(), String.class);
            }
        }
        throw new RuntimeException("Cannot find HGData new technologies column definition.");
    }

    private Node aggregateNewTechs(Node source, List<SourceColumn> columns) {
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(domainField, String.class));
        fms.add(newTechFieldMetadata(columns));
        Buffer<?> buffer = new HGDataNewTechBuffer(domainField, newTechFieldMetadata(columns).getFieldName());
        Node newTech = source.groupByAndBuffer(new FieldList(domainField), buffer, fms);
        return newTech.renamePipe("newTech");
    }

}
