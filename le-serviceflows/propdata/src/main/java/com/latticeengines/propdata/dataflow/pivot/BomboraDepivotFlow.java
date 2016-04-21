package com.latticeengines.propdata.dataflow.pivot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.DepivotDataFlowParameters;
import com.latticeengines.propdata.core.IngenstionNames;

@Component("bomboraDepivotFlow")
public class BomboraDepivotFlow extends TypesafeDataFlowBuilder<DepivotDataFlowParameters> {

    @Override
    public Node construct(DepivotDataFlowParameters parameters) {

        Node node = addSource(IngenstionNames.BOMBORA_FIREHOSE);
        String[] targetFields = new String[] { "Topic", "TopicScore" };
        String[][] sourceFieldTuples = new String[][] { //
                { "Topic1", "Topic1Score" }, //
                { "Topic2", "Topic2Score" }, //
                { "Topic3", "Topic3Score" }, //
                { "Topic4", "Topic4Score" }, //
                { "Topic5", "Topic5Score" }, //
                { "Topic6", "Topic6Score" }, //
                { "Topic7", "Topic7Score" }, //
                { "Topic8", "Topic8Score" }, //
                { "Topic9", "Topic9Score" }, //
                { "Topic10", "Topic10Score" } //
        };
        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        fieldMetadataList.add(new FieldMetadata("Topic1", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic1Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic2", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic3Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic3", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic3Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic4", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic4Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic5", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic5Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic6", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic6Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic7", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic7Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic8", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic8Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic9", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic9Score", Double.class));
        fieldMetadataList.add(new FieldMetadata("Topic10", String.class));
        fieldMetadataList.add(new FieldMetadata("Topic10Score", Double.class));
        node.setSchema(fieldMetadataList);

        return node.depivot(targetFields, sourceFieldTuples);
    }
}