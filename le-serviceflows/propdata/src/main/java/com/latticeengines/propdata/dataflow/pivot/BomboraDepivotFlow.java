package com.latticeengines.propdata.dataflow.pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.dataflow.DepivotDataFlowParameters;
import com.latticeengines.propdata.core.IngenstionNames;

@Component("bomboraDepivotFlow")
public class BomboraDepivotFlow extends TypesafeDataFlowBuilder<DepivotDataFlowParameters> {
    private static final String SCHEMA_BOMBORA_FIREHOSE = "classpath:schema/BomboraFirehoseAvroSchema.avsc";
    @Autowired
    private ResourceLoader resourceLoader;

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

        setSchemaToNode(node);

        return node.depivot(targetFields, sourceFieldTuples);
    }

    private void setSchemaToNode(Node node) {
        Schema schema;
        try {
            schema = AvroUtils.readSchemaFromResource(resourceLoader, SCHEMA_BOMBORA_FIREHOSE);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25018, e);
        }

        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        for (Field field : schema.getFields()) {
            for (Schema fieldType : field.schema().getTypes()) {
                if (fieldType.getType() == Type.NULL) {
                    continue;
                }
                Class<?> javaType = AvroUtils.getJavaType(fieldType.getType());
                fieldMetadataList.add(new FieldMetadata(field.name(), javaType));
                break;
            }
        }
        node.setSchema(fieldMetadataList);
    }
}