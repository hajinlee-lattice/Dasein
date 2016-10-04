package com.latticeengines.propdata.dataflow.refresh;

import cascading.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrGroupCountBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.CharacterizationParameters;

@Component("characterizationFlow")
public class CharacterizationFlow extends TypesafeDataFlowBuilder<CharacterizationParameters> {
    private static final Log log = LogFactory.getLog(CharacterizationFlow.class);

    @Override
    public Node construct(CharacterizationParameters parameters) {

        log.info("Add account master as base");

        List<String> groupKeys = parameters.getGroupKeys();
        String versionKey = parameters.getVersionKey();
        String attrKey = parameters.getAttrKey();
        String categoryKey = parameters.getCategoryKey();
        String countKey = parameters.getCountKey();
        String percentKey = parameters.getPercentKey();
        Long totalRecords = parameters.getTotalRecords();
        String version = parameters.getVersion();
        List<String> attrList = parameters.getAttrs();
        String[] attrs = attrList.toArray(new String[attrList.size()]);
        List<String> categoryList = parameters.getCategories();
        String[] categories = categoryList.toArray(new String[categoryList.size()]);

        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(versionKey, String.class));
        fms.add(new FieldMetadata(attrKey, String.class));
        fms.add(new FieldMetadata(categoryKey, String.class));
        fms.add(new FieldMetadata(countKey, Long.class));
        fms.add(new FieldMetadata(percentKey, Float.class));

        Fields groupFields = new Fields(versionKey, attrKey, categoryKey, countKey, percentKey);
        for (int i = 0; i < groupKeys.size(); i++) {
            groupFields = groupFields.append(new Fields(groupKeys.get(i)));
            fms.add(new FieldMetadata(groupKeys.get(i), String.class));
        }

        Node accountMaster = addSource(parameters.getBaseTables().get(0));
        AttrGroupCountBuffer buffer = new AttrGroupCountBuffer(attrs, categories, groupFields, version, totalRecords.longValue(),
                                                               versionKey, attrKey, categoryKey, countKey, percentKey);

        Node grouped = accountMaster.groupByAndBuffer(new FieldList(groupKeys), buffer, fms);

        return grouped;
    }
}
