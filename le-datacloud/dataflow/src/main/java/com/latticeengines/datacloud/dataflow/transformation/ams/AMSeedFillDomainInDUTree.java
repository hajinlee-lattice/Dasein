package com.latticeengines.datacloud.dataflow.transformation.ams;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.AMSeedDUFillDomainFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ams.AMSeedDUPriDomBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

import cascading.tuple.Fields;

@Component(AMSeedFillDomainInDUTree.DATAFLOW_BEAN_NAME)
public class AMSeedFillDomainInDUTree extends ConfigurableFlowBase<TransformerConfig> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AMSeedFillDomainInDUTree.class);

    public static final String DATAFLOW_BEAN_NAME = "AMSeedFillDomainInDUFlow";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_AMS_FILL_DOM_IN_DU;

    public static final String DU_DUNS = DataCloudConstants.ATTR_DU_DUNS;
    public static final String DUNS = DataCloudConstants.AMS_ATTR_DUNS;
    public static final String OPT_LOG = OperationLogUtils.DEFAULT_FIELD_NAME;
    public static final String DU_OPT_LOG = "_DU_OperationLogs_";

    /*
     * Find primary domain in DU tree by prioritized rules and then populate
     * empty domains with primary domain in DU tree
     */
    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node ams = addSource(parameters.getBaseTables().get(0));
        if (ams.getSchema(OPT_LOG) == null) {
            ams = ams.addColumnWithFixedValue(OPT_LOG, null, String.class);
        }
        List<String> finalFields = new ArrayList<>(ams.getFieldNames());

        // Look for primary domain for each DU tree
        Fields duAndDomain = new Fields(new String[] { DU_DUNS, AMSeedDUPriDomBuffer.DU_PRIMARY_DOMAIN });
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DU_DUNS, String.class));
        fms.add(new FieldMetadata(AMSeedDUPriDomBuffer.DU_PRIMARY_DOMAIN, String.class));
        Node duDomain = ams.discard(new FieldList(OPT_LOG)) // duDomain node needs to reset log
                .filter(DU_DUNS + " != null", new FieldList(DU_DUNS)) //
                // TODO: Actually based on the use case, use aggregator here is
                // better
                .groupByAndBuffer(new FieldList(DU_DUNS), new AMSeedDUPriDomBuffer(duAndDomain), fms, true) //
                .rename(new FieldList(OPT_LOG), new FieldList(DU_OPT_LOG)) //
                .renamePipe("dudomain");

        // Populate empty domains in DU tree in AMSeed
        Node join = ams.leftJoin(DU_DUNS, duDomain, DU_DUNS);
        AMSeedDUFillDomainFunction func = new AMSeedDUFillDomainFunction(new Fields(join.getFieldNamesArray()),
                DU_OPT_LOG);
        Node domainFilled = join.apply(func, new FieldList(join.getFieldNames()), join.getSchema(),
                new FieldList(join.getFieldNames()), Fields.REPLACE);

        // Finalize schema
        domainFilled = domainFilled.retain(new FieldList(finalFields));

        return domainFilled;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;

    }
}
