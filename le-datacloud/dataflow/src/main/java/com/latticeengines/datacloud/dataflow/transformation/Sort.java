package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.Sort.BEAN_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.SortPartitionBuffer;
import com.latticeengines.dataflow.runtime.cascading.SortPartitionFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.SorterParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Buffer;
import cascading.operation.Function;

@Component(BEAN_NAME)
public class Sort extends TypesafeDataFlowBuilder<SorterParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(Sort.class);

    public static final String BEAN_NAME = "sort";

    private static final String DUMMY_GROUP = "_Sort_Dummy_Group_";
    private static final String SORTED_GROUPS = "_Sorted_Groups_";

    @SuppressWarnings("unchecked")
    @Override
    public Node construct(SorterParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        if (parameters.getPartitions() == 1) {
            return source.sort(parameters.getSortingField());
        } else {
            List<String> fieldsToRetain = new ArrayList<>(source.getFieldNames());

            // add dummy group to put all in one group
            source = source.addColumnWithFixedValue(DUMMY_GROUP, UUID.randomUUID().toString(), String.class);
            // find partition boundaries
            Node boundaries = sortAndDivide(source, parameters).renamePipe("boundaries");
            source = source.hashJoin(new FieldList(DUMMY_GROUP), boundaries, new FieldList(DUMMY_GROUP), JoinType.LEFT);
            // mark partition
            FieldMetadata sortingFm = source.getSchema(parameters.getSortingField());
            source = markPartition(source, parameters.getSortingField(), parameters.getPartitionField(),
                    (Class<Comparable<?>>) sortingFm.getJavaType());
            fieldsToRetain.add(parameters.getPartitionField());
            // group by and retain
            source = source.groupByAndRetain(new FieldList(fieldsToRetain),
                    new FieldList(parameters.getPartitionField()), new FieldList(parameters.getSortingField()));
            return source;
        }
    }

    private Node sortAndDivide(Node node, SorterParameters parameters) {
        node = node.retain(new FieldList(parameters.getSortingField(), DUMMY_GROUP));
        String sortField = parameters.getSortingField();
        Buffer buffer = new SortPartitionBuffer(sortField, DUMMY_GROUP, SORTED_GROUPS, parameters.getPartitions());
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DUMMY_GROUP, String.class));
        fms.add(new FieldMetadata(SORTED_GROUPS, String.class));
        return node.groupByAndBuffer(new FieldList(DUMMY_GROUP), new FieldList(sortField), buffer, fms);
    }

    private Node markPartition(Node node, String sortingField, String partitionField,
            Class<Comparable<?>> sortingFieldClz) {
        Function function = new SortPartitionFunction(partitionField, SORTED_GROUPS, sortingField, sortingFieldClz);
        List<String> outputFields = new ArrayList<>(node.getFieldNames());
        outputFields.add(partitionField);
        return node.apply(function, new FieldList(SORTED_GROUPS, sortingField),
                new FieldMetadata(partitionField, Integer.class), new FieldList(outputFields));
    }

}
