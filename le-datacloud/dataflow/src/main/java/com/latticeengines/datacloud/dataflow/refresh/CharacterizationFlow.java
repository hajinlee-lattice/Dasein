package com.latticeengines.datacloud.dataflow.refresh;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AttrGroupCountBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.CharacterizationParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("characterizationFlow")
public class CharacterizationFlow extends TypesafeDataFlowBuilder<CharacterizationParameters> {
    private static final Logger log = LoggerFactory.getLogger(CharacterizationFlow.class);

    @Override
    public Node construct(CharacterizationParameters parameters) {

        log.info("Add account master as base");

        List<String> groupKeys = parameters.getGroupKeys();
        String versionKey = parameters.getVersionKey();
        String[] attrKey = parameters.getAttrKey();
        String totalKey = parameters.getTotalKey();
        String version = parameters.getVersion();
        List<String> attrList = parameters.getAttrs();
        List<Integer> attrIdList = parameters.getAttrIds();
        String[] attrs = attrList.toArray(new String[attrList.size()]);
        Integer[] attrIds = attrIdList.toArray(new Integer[attrList.size()]);
        for (int i = 0; i < attrs.length; i++) {
            log.info(i + ". Attribute " + attrs[i] + " id " + attrIds[i]);
        }

        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(versionKey, String.class));
        fms.add(new FieldMetadata(totalKey, Long.class));
        Fields groupFields = new Fields(versionKey, totalKey);
        for (int i = 0; i < attrKey.length; i++) {
            fms.add(new FieldMetadata(attrKey[i], String.class));
            groupFields = groupFields.append(new Fields(attrKey[i]));
        }

        for (int i = 0; i < groupKeys.size(); i++) {
            groupFields = groupFields.append(new Fields(groupKeys.get(i)));
            fms.add(new FieldMetadata(groupKeys.get(i), String.class));
        }

        Node accountMaster = addSource(parameters.getBaseTables().get(0));
        AttrGroupCountBuffer buffer = new AttrGroupCountBuffer(attrs, attrIds, groupFields, version,
                                                               versionKey, attrKey, totalKey);

        Node grouped = accountMaster.groupByAndBuffer(new FieldList(groupKeys), buffer, fms);

        return grouped;
    }
}
