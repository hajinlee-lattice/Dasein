package com.latticeengines.propdata.dataflow.pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.dataflow.runtime.cascading.StringTruncateFunction;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.dataflow.DepivotDataFlowParameters;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.IngenstionNames;

import cascading.operation.Function;

@Component("bomboraDepivotFlow")
public class BomboraDepivotFlow extends TypesafeDataFlowBuilder<DepivotDataFlowParameters> {
    private static final Log LOG = LogFactory.getLog(BomboraDepivotFlow.class);
    private static final String LE_TIMESTAMP = "LE_Last_Upload_Date";
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

        node = node.depivot(targetFields, sourceFieldTuples);
        node = node.addTimestamp(LE_TIMESTAMP);
        node = addTruncateLogic(node, parameters.getColumns());
        return node;
    }

    private Node addTruncateLogic(Node node, List<SourceColumn> columns) {
        node = addTruncateNode(node, "Domain", 512);
        node = addTruncateNode(node, "HashedEmailIDBase64", 256);
        node = addTruncateNode(node, "Topic", 256);
        node = addTruncateNode(node, "UniversalDateTime", 256);
        node = addTruncateNode(node, "Country", 128);
        node = addTruncateNode(node, "StateRegion", 128);
        node = addTruncateNode(node, "PostalCode", 32);
        node = addTruncateNode(node, "InteractionType", 32);
        node = addTruncateNode(node, "SourceID", 4000);
        node = addTruncateNode(node, "CustomID", 4000);
        return node;
    }

    private Node addTruncateNode(Node node, String columnName, int maxLength) {
        Function<?> function = new StringTruncateFunction(columnName, maxLength);
        node = node.apply(function, new FieldList(columnName), new FieldMetadata(columnName, String.class));
        return node;
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