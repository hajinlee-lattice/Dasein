package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.AMStatsHQDuns.BEAN_NAME;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DDUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.GDUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.HQ_DUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.HQ_DUNS_DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.STATUS_CODE;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.SUBSIDIARY_INDICATOR;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsHQDunsFunction2;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

/**
 * Add HQ_DUNS column to am.
 */
@Component(BEAN_NAME)
public class AMStatsHQDuns extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "amStatsHQDuns";
    public static final String TRANSFORMER_NAME = "amStatsHQDunsCalculator";
    private static final String latticeAccountId = InterfaceName.LatticeAccountId.name();

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node am, amBkt;
        if (parameters.getBaseTables().size() > 1) {
            amBkt = addSource(parameters.getBaseTables().get(0)).renamePipe("ambkt");
            am = addSource(parameters.getBaseTables().get(1));
        } else {
            am = addSource(parameters.getBaseTables().get(0));
            amBkt = am;
        }
        String amId = am.getFieldNames().contains(latticeAccountId)? latticeAccountId : DataCloudConstants.LATTICE_ID;
        am = am.retain(new FieldList(amId, //
                DOMAIN, //
                STATUS_CODE, //
                SUBSIDIARY_INDICATOR, //
                DUNS, //
                DDUNS, //
                GDUNS //
        )).rename(new FieldList(DataCloudConstants.LATTICE_ID), //
                new FieldList(InterfaceName.LatticeAccountId.name()));
        Node nodeWithProperCodes = am
                .filter(DOMAIN + " != null && " + STATUS_CODE + " != null && " + SUBSIDIARY_INDICATOR + " != null && "
                        + DUNS + " != null", new FieldList(DOMAIN, STATUS_CODE, SUBSIDIARY_INDICATOR, DUNS))
                .renamePipe("hqduns");
        Node hqduns = addHQDunsValues(nodeWithProperCodes);
        hqduns = hqduns.rename(new FieldList(DOMAIN), new FieldList(HQ_DUNS_DOMAIN));
        hqduns = hqduns.retain(latticeAccountId, HQ_DUNS_DOMAIN, HQ_DUNS);
        if (parameters.getBaseTables().size() > 1) {
            return amBkt.leftJoin(new FieldList(latticeAccountId), hqduns, new FieldList(latticeAccountId));
        } else {
            return hqduns;
        }
    }

    private Node addHQDunsValues(Node nodeWithProperCodes) {
        AMStatsHQDunsFunction2.Params functionParams = AMStatsHQDunsFunction2.paramsBuilder() //
                .statusCodeField(STATUS_CODE) //
                .subIndicatorField(SUBSIDIARY_INDICATOR) //
                .dunsField(DUNS) //
                .ddunsField(DDUNS) //
                .gdunsField(GDUNS) //
                .hqDunsField(HQ_DUNS) //
                .build();
        AMStatsHQDunsFunction2 hqDunsCalculationFunction = new AMStatsHQDunsFunction2(functionParams);

        return nodeWithProperCodes.apply(hqDunsCalculationFunction, //
                new FieldList(functionParams.applyToFields()), //
                new FieldMetadata(HQ_DUNS, String.class));
    }

    @Override
    public Class<TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
