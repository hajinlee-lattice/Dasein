package com.latticeengines.datacloud.dataflow.transformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedDeriveAttrsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(AMSeedDeriveAttrs.DATAFLOW_BEAN_NAME)
public class AMSeedDeriveAttrs extends AccountMasterBase<AMSeedDeriveAttrsConfig> {
    public final static String DATAFLOW_BEAN_NAME = "AMSeedDeriveAttrs";
    public final static String TRANSFORMER_NAME = "AMSeedDeriveAttrsTransformer";
    private final static String DU_SALES_VOLUME = "DOMESTIC_HQ_SALES_VOLUME";
    private final static String GU_SALES_VOLUME = "GLOBAL_HQ_SALES_VOLUME";
    private final static String PARENT_SALES_VOLUME = "PARENTS_SALES_VOLUME";

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
        return AMSeedDeriveAttrsConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        AMSeedDeriveAttrsConfig config = getTransformerConfig(parameters);
        Node amSeedDataSet = addSource(parameters.getBaseTables().get(0));

        String checkNullDuns = config.getAmSeedDuns() + " != null && !" + config.getAmSeedDuns()
        + ".equals(\"\")";
        List<String> fieldNamesSet = amSeedDataSet.getFieldNames();

        Node amSeedSales = amSeedDataSet //
                .filter(checkNullDuns, new FieldList(config.getAmSeedDuns())) //
                .retain(new FieldList(config.getAmSeedDuns(), config.getUsSalesVolume())) //
                .groupByAndLimit(new FieldList(config.getAmSeedDuns()), 1) //
                .rename(new FieldList(config.getUsSalesVolume(), config.getAmSeedDuns()),
                        new FieldList("amSeedSales_" + config.getUsSalesVolume(),
                                "amSeedSales_" + config.getAmSeedDuns())) //
                .retain(new FieldList("amSeedSales_" + config.getUsSalesVolume(),
                        "amSeedSales_" + config.getAmSeedDuns())) //
                .renamePipe("amSeedSales");

        Map<String, String> targetDunsMap = new HashMap<String, String>() {
            {
                put(config.getAmSeedDuDuns(), DU_SALES_VOLUME);
                put(config.getAmSeedGuDuns(), GU_SALES_VOLUME);
                put(config.getAmSeedParentDuns(), PARENT_SALES_VOLUME);
            }
        };

        Node resultNode = amSeedDataSet;
        for (Map.Entry<String, String> targetDuns : targetDunsMap.entrySet()) {
            String withTargetDuns = targetDuns.getKey() + " != null && !" + targetDuns.getKey() + ".equals(\"\")";
            String withNoTargetDuns = targetDuns.getKey() + " == null || " + targetDuns.getKey() + ".equals(\"\")";
            Node amSeedWithTgtDuns = resultNode //
                    .renamePipe("amSeedWithTgtDuns");
            Node amSeedWithNoTgtDuns = resultNode //
                    .renamePipe("amSeedWithNoTgtDuns");
            amSeedWithTgtDuns = amSeedWithTgtDuns //
                    .filter(withTargetDuns, new FieldList(targetDuns.getKey()));
            amSeedWithNoTgtDuns = amSeedWithNoTgtDuns //
                    .filter(withNoTargetDuns, new FieldList(targetDuns.getKey()));
            fieldNamesSet.add(targetDuns.getValue());
            amSeedWithNoTgtDuns = amSeedWithNoTgtDuns //
                    .addColumnWithFixedValue(targetDuns.getValue(), null, Long.class) //
                    .retain(new FieldList(fieldNamesSet));
            amSeedWithTgtDuns = amSeedWithTgtDuns //
                    .join(targetDuns.getKey(), amSeedSales, "amSeedSales_" + config.getAmSeedDuns(), JoinType.LEFT) //
                    .rename(new FieldList("amSeedSales_" + config.getUsSalesVolume()),
                            new FieldList(targetDuns.getValue())) //
                    .retain(new FieldList(fieldNamesSet));
            resultNode = amSeedWithTgtDuns //
                    .merge(amSeedWithNoTgtDuns);
        }
        return resultNode;
    }

}
