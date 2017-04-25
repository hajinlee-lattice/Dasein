package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public abstract class AMStatsFlowBase
        extends TransformationFlowBase<BasicTransformationConfiguration, AccountMasterStatsParameters> {
    protected static final String TEMP_RENAMED_PREFIX = "_RENAMED_";
    protected static final String MIN_MAX_JOIN_FIELD = "_JoinFieldMinMax_";
    protected static final String MIN_MAX_JOIN_FIELD_RENAMED = TEMP_RENAMED_PREFIX + "_JoinFieldMinMax_";

    @Autowired
    protected ColumnMetadataProxy columnMetadataProxy;

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    protected FieldList getFieldList(List<FieldMetadata> fieldMetadataList) {
        List<String> fields = new ArrayList<>();
        for (FieldMetadata field : fieldMetadataList) {
            fields.add(field.getFieldName());
        }
        FieldList fieldList = new FieldList(fields);
        return fieldList;
    }

    protected String getTotalKey() {
        return AccountMasterStatsParameters.GROUP_TOTAL_KEY_TEMP;
    }

    protected String getMinMaxKey() {
        return AccountMasterStatsParameters.MIN_MAX_KEY;
    }

}
