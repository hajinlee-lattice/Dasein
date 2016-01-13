package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;

import cascading.operation.Buffer;

@Component("hgDataPivotFlow")
public class HGDataPivotFlow extends PivotFlow {

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private String domainField;

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        domainField = parameters.getJoinFields()[0];

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

        Node join = joinedConfigurablePipes(parameters, sourceMap);
        Node newTech = aggregateNewTechs(last4Month, parameters.getColumns());

        FieldList joinFieldList = new FieldList(parameters.getJoinFields());
        join = join.join(joinFieldList, newTech, joinFieldList, JoinType.OUTER);
        join = join.addTimestamp(parameters.getTimestampField());
        return finalRetain(join, parameters.getColumns());
    }

    private Node aggregateNewTechs(Node source, List<SourceColumn> columns) {
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(domainField, String.class));
        fms.add(newTechFieldMetadata(columns));
        Buffer<?> buffer = new HGDataNewTechBuffer(domainField, newTechFieldMetadata(columns).getFieldName());
        Node newTech = source.groupByAndBuffer(new FieldList(domainField), buffer, fms);
        return newTech.renamePipe("newTech");
    }

    private FieldMetadata newTechFieldMetadata(List<SourceColumn> columns) {
        for (SourceColumn column: columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.HGDATA_NEWTECH)) {
                return new FieldMetadata(column.getColumnName(), String.class);
            }
        }
        throw new RuntimeException("Cannot find HGData new technologies column definition.");
    }

}
