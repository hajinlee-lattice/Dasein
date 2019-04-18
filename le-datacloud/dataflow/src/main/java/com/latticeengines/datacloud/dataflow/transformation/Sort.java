package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.Sort.BEAN_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.SortPartitionBuffer;
import com.latticeengines.dataflow.runtime.cascading.SortPartitionFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.SorterParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(BEAN_NAME)
public class Sort extends TypesafeDataFlowBuilder<SorterParameters> {

    public static final String BEAN_NAME = "sort";

    private static final String DUMMY_GROUP = "_Sort_Dummy_Group_";
    private static final String DUMMY_JOIN_KEY = "_Sort_Dummy_Join_Key_";
    private static final String SORTED_GROUPS = "_Sorted_Groups_";

    @SuppressWarnings("unchecked")
    @Override
    public Node construct(SorterParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        if (parameters.getPartitions() == 1) {
            return source.sort(parameters.getSortingField());
        } else {
            List<String> fieldsToRetain = new ArrayList<>(source.getFieldNames());
            int numJoinKeys = Math.min(Math.max(parameters.getPartitions() * 10, 256), 10240);
            // add dummy group to put all in one group
            source = source.addColumnWithFixedValue(DUMMY_GROUP, UUID.randomUUID().toString(), String.class);
            source = source.apply(String.format("System.currentTimeMillis() %% %d", numJoinKeys),
                    new FieldList(parameters.getSortingField()), new FieldMetadata(DUMMY_JOIN_KEY, Long.class));
            // find partition boundaries
            Node boundaries = profile(source, parameters).renamePipe("boundaries").checkpoint("boundaries");
            source = source.leftJoin(new FieldList(DUMMY_JOIN_KEY), boundaries, new FieldList(DUMMY_JOIN_KEY));
            // mark partition id
            FieldMetadata sortingFm = source.getSchema(parameters.getSortingField());
            source = markPartition(source, parameters.getSortingField(), parameters.getPartitionField(),
                    (Class<Comparable<?>>) sortingFm.getJavaType());
            fieldsToRetain.add(parameters.getPartitionField());
            // group by partition id and retain original fields + partition id
            source = source.groupByAndRetain(new FieldList(fieldsToRetain),
                    new FieldList(parameters.getPartitionField()), new FieldList(parameters.getSortingField()));
            return source;
        }
    }

    private Node profile(Node source, SorterParameters parameters) {
        Node node = source.retain(new FieldList(parameters.getSortingField(), DUMMY_GROUP, DUMMY_JOIN_KEY));
        String sortField = parameters.getSortingField();
        Class<?> sortFieldClz = source.getSchema(sortField).getJavaType();
        SortPartitionBuffer buffer = new SortPartitionBuffer(sortField, DUMMY_JOIN_KEY, SORTED_GROUPS, sortFieldClz,
                parameters.getPartitions());
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DUMMY_JOIN_KEY, String.class));
        fms.add(new FieldMetadata(SORTED_GROUPS, String.class));
        return node.groupByAndBuffer(new FieldList(DUMMY_GROUP), new FieldList(sortField), buffer, fms);
    }

    private Node markPartition(Node node, String sortingField, String partitionField,
            Class<Comparable<?>> sortingFieldClz) {
        SortPartitionFunction function = new SortPartitionFunction(partitionField, SORTED_GROUPS, sortingField,
                sortingFieldClz);
        List<String> outputFields = new ArrayList<>(node.getFieldNames());
        outputFields.add(partitionField);
        return node.apply(function, new FieldList(SORTED_GROUPS, sortingField),
                new FieldMetadata(partitionField, Integer.class), new FieldList(outputFields));
    }

}
