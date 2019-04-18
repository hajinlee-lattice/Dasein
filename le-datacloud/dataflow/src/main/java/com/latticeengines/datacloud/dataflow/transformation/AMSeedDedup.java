package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedDedupBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMSeedDedupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

import cascading.tuple.Fields;

@Component(AMSeedDedup.DATAFLOW_BEAN_NAME)
public class AMSeedDedup extends ConfigurableFlowBase<AMSeedDedupConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedDedup";
    public static final String TRANSFORMER_NAME = "AMSeedDeduper";

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
        return AMSeedDedupConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AMSeedDedupConfig config = getTransformerConfig(parameters);
        Node amSeed = addSource(parameters.getBaseTables().get(0));
        if (config.getIsDunsOnlyCleanup()) {
            amSeed = cleanup(amSeed, DataCloudConstants.AMS_ATTR_DUNS, DataCloudConstants.AMS_ATTR_DOMAIN);
        }
        if (config.getIsDomainOnlyCleanup()) {
            amSeed = cleanup(amSeed, DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS);
        }
        if (config.getIsDedup()) {
            amSeed = amSeed.groupByAndLimit(
                    new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS), 1);
        }
        return amSeed;
    }

    private Node cleanup(Node amSeed, String groupbyField, String checkingField) {
        // Separate AMSeed to 2 parts: AMSeedWithGroupByField &
        // AMSeedNullGroupByField
        String colValPresent = groupbyField + " != null";
        String colValNotPresent = groupbyField + " == null";
        Node amSeedToClean = amSeed //
                .filter(colValPresent, new FieldList(groupbyField));
        Node amSeedToRetain = amSeed //
                .filter(colValNotPresent, new FieldList(groupbyField));
        amSeedToClean = amSeedToClean.groupByAndBuffer(new FieldList(groupbyField),
                new AMSeedDedupBuffer(new Fields(amSeed.getFieldNamesArray()), checkingField),
                amSeed.getSchema());
        amSeed = amSeedToClean //
                .merge(amSeedToRetain);
        return amSeed;
    }

}
