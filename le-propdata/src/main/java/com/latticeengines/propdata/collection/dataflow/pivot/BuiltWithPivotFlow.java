package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;
import com.latticeengines.propdata.collection.source.impl.BuiltWithPivoted;

@Component("builtWithPivotFlow")
public class BuiltWithPivotFlow extends PivotFlow {

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    @Autowired
    BuiltWithPivoted bwPivoted;

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        Map<String, Node> sourceMap = new HashMap<>();
        sourceMap.put(parameters.getBaseTables().get(0), source);

        Node current = source.filter("Technology_Last_Detected + " + ONE_MONTH + "L >= LE_Last_Upload_Date",
                new FieldList("Technology_Last_Detected", "LE_Last_Upload_Date"));
        current = current.renamePipe("current");
        sourceMap.put(parameters.getBaseTables().get(0) + "_Current", current);

        PivotStrategy pivotStrategy = new BuiltWithPivotStrategy();
        Node pivot = source.pivot(new String[]{ "Domain" }, pivotStrategy);

        for (String typeField: BuiltWithPivotStrategy.typeFlags) {
            pivot = pivot.renameBooleanField(typeField, BooleanType.TRUE_FALSE);
        }

        List<SourceColumn> columns = parameters.getColumns();
        String[] joinFields = parameters.getJoinFields();
        List<Node> pivotedPipes = pivotPipes(columns, sourceMap, joinFields);
        Node join = joinPipe(joinFields, pivotedPipes.toArray(new Node[pivotedPipes.size()]));
        join = join.renamePipe("join");

        FieldList joinList = new FieldList(joinFields);
        pivot = pivot.innerJoin(joinList, join, joinList);

        return pivot.addTimestamp(bwPivoted.getTimestampField());
    }

}
