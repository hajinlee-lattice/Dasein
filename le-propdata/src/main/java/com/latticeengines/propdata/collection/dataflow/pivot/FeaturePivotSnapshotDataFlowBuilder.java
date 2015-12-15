package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.runtime.cascading.PivotBuffer;

import cascading.tuple.Fields;

@Component("featurePivotSnapshotDataFlowBuilder")
@Scope("prototype")
public class FeaturePivotSnapshotDataFlowBuilder extends PivotSnapshotDataFlowBuilder {

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplateDest")
    protected JdbcTemplate jdbcTemplateDest;

    @Override
    protected FieldList getGroupbyFields() { return new FieldList("URL"); }

    @Override
    protected List<FieldMetadata> getFieldMetadatas() {
        List<Map<String, Object>> results = jdbcTemplateDest.queryForList(
                "SELECT [Feature], [Result_Column_Type] FROM [FeatureManagement_FeaturePivot]");
        List<FieldMetadata> metadatas = new ArrayList<>();
        metadatas.add(new FieldMetadata("URL", String.class));
        for (Map<String, Object> result: results) {
            String feature = (String) result.get("Feature");
            String type = (String) result.get("Result_Column_Type");
            FieldMetadata metadata;
            if ("INT".equalsIgnoreCase(type)) {
                metadata = new FieldMetadata(feature, Integer.class);
            } else {
                metadata = new FieldMetadata(feature, String.class);
            }
            metadatas.add(metadata);
        }
        metadatas.add(new FieldMetadata("Timestamp", Long.class));
        return metadatas;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected PivotBuffer getPivotBuffer() {
        List<FieldMetadata> fms = getFieldMetadatas();
        List<Class> formats = new ArrayList<>();
        Map<String, String> valueColumnMap = new HashMap<>();
        String[] fields = new String[fms.size()];
        for (int i = 0; i< fms.size(); i++) {
            FieldMetadata metadata = fms.get(i);
            fields[i] = metadata.getFieldName();
            formats.add(metadata.getJavaType());
            valueColumnMap.put(metadata.getFieldName(), metadata.getFieldName());
        }
        return new FeaturePivotBuffer(formats, valueColumnMap, new Fields(fields));
    }

}
