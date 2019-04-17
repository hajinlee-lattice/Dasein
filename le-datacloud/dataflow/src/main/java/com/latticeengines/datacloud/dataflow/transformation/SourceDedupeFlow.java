package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrMergeBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SourceDedupeTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("sourceDedupeFlow")
public class SourceDedupeFlow extends ConfigurableFlowBase<SourceDedupeTransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(SourceDedupeFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceDedupeTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        String dedupeField = config.getDedupeField();

        List<String> fieldNames = source.getFieldNames();
        Fields fieldDec = new Fields();

        for (String fieldName : fieldNames) {
            log.info("Add field " + fieldName);
            fieldDec = fieldDec.append(new Fields(fieldName));
        }

        List<FieldMetadata> fms = source.getSchema();

        AttrMergeBuffer buffer = new AttrMergeBuffer(fieldDec);

        Node grouped = source.groupByAndBuffer(new FieldList(dedupeField), buffer, fms);

        return grouped;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceDedupeTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceDedupeFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceDeduper";

    }
}
