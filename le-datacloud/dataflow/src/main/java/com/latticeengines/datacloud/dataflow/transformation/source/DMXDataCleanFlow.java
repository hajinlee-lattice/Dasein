package com.latticeengines.datacloud.dataflow.transformation.source;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.DMXDataCleanConfig;

@Component(DMXDataCleanFlow.DATAFLOW_BEAN_NAME)
public class DMXDataCleanFlow extends ConfigurableFlowBase<DMXDataCleanConfig> {
    public static final String DATAFLOW_BEAN_NAME = "dmxDataCleanFlow";
    public static final String TRANSFORMER_NAME = "dmxDataCleanTransformer";
    
    private DMXDataCleanConfig config;
    
    public static final String FINAL_DUNS = "Duns";
    public static final String FINAL_INTENSITY = "intensity";
    public static final String FINAL_SUPPLIER_NAME = "Supplier_Name";
    public static final String FINAL_SEGMENT_NAME = "Segment_Name";
    public static final String FINAL_CATEGORY = "Category";
    public static final String FINAL_DESCRIPTION = "Description";
    public static final String FINAL_COLLECTION_NAME = "Collection_Name";
    
    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DMXDataCleanConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        
        // Filter as per RecordType and add mapping
        // Type = Product
        Node prodTechInd = source
                .filter(config.getRecordTypeField() + ".equals(\"P\")", new FieldList(config.getRecordTypeField())) //
                .rename(new FieldList(config.getProductField()), new FieldList(FINAL_SEGMENT_NAME)) //
                .retain(new FieldList(config.getDunsField(), config.getIntensityField(), FINAL_SEGMENT_NAME)) //
                .addColumnWithFixedValue(FINAL_SUPPLIER_NAME, null, String.class) //
                .addColumnWithFixedValue(FINAL_COLLECTION_NAME, null, String.class);
        // Type = Vendor
        Node vendTechInd = source
                .addColumnWithFixedValue(FINAL_SEGMENT_NAME, null, String.class) //
                .filter(config.getRecordTypeField() + ".equals(\"V\")", new FieldList(config.getRecordTypeField())) //
                .rename(new FieldList(config.getRecordValueField()), new FieldList(FINAL_SUPPLIER_NAME)) //
                .retain(new FieldList(config.getDunsField(), config.getIntensityField(), FINAL_SEGMENT_NAME, FINAL_SUPPLIER_NAME)) //
                .addColumnWithFixedValue(FINAL_COLLECTION_NAME, null, String.class);
        // Type = Solution
        Node solTechInd = source
                .addColumnWithFixedValue(FINAL_SEGMENT_NAME, null, String.class) //
                .addColumnWithFixedValue(FINAL_SUPPLIER_NAME, null, String.class) //
                .filter(config.getRecordTypeField() + ".equals(\"S\")", new FieldList(config.getRecordTypeField())) //
                .rename(new FieldList(config.getRecordValueField()), new FieldList(FINAL_COLLECTION_NAME)) //
                .retain(new FieldList(config.getDunsField(), config.getIntensityField(), FINAL_SEGMENT_NAME, FINAL_SUPPLIER_NAME, FINAL_COLLECTION_NAME));
        
        // Merge Data
        Node mergedData = prodTechInd //
                .merge(vendTechInd) //
                .merge(solTechInd);
        return mergedData;
    }

}
