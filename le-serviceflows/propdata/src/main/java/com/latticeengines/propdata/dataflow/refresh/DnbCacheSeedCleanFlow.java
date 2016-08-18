package com.latticeengines.propdata.dataflow.refresh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainMergeAndCleanFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.CleanDataFlowParameter;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

@Component("dnbCacheSeedCleanFlow")
public class DnbCacheSeedCleanFlow extends TypesafeDataFlowBuilder<CleanDataFlowParameter> {
    @Override
    public Node construct(CleanDataFlowParameter parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        setSchemaToNode(node, parameters.getBaseSourceColumns().get(0));
        // node = DataFlowUtils.normalizeDomain(node, "LE_DOMAIN");
        List<String> domainFieldNames = Arrays.asList("LE_DOMAIN");
        node = node.apply(new DomainMergeAndCleanFunction(domainFieldNames, "LE_DOMAIN"),
                new FieldList(domainFieldNames), new FieldMetadata("LE_DOMAIN", String.class));
        return node;
    }

    private void setSchemaToNode(Node node, List<SourceColumn> baseSourceColumns) {
        CsvToAvroFieldMapping fieldMapping = new CsvToAvroFieldMappingImpl(baseSourceColumns);
        Schema schema = fieldMapping.getAvroSchema();

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
