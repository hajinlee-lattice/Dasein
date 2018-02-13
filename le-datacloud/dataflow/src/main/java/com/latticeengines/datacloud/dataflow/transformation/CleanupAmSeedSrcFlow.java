package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.CleanAmSeedWithDomOwnTabFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ComputeRootDunsAndTypeFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(CleanupAmSeedSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupAmSeedSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupAmSeedSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupAmSeedSrcTransformer";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private static List<String> fieldList;

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
        return FormDomOwnershipTableConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node domOwnershipTable = addSource(parameters.getBaseTables().get(0));
        Node amSeed = addSource(parameters.getBaseTables().get(1));
        // Filter the records that are not to be used for cleanup
        String filterDomOwnTable = ROOT_DUNS + " != null";
        fieldList = amSeed.getFieldNames();
        // retaining required columns : domain, root duns, dunsType
        Node domOwnTableForCleanup = domOwnershipTable //
                .filter(filterDomOwnTable, new FieldList(ROOT_DUNS)) //
                .rename(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS),
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), renameField(ROOT_DUNS))) //
                .retain(new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN), renameField(ROOT_DUNS),
                        DUNS_TYPE));
        Node finalCleanedupAmSeed = computeRootDunsAndCompare(amSeed, domOwnTableForCleanup);
        Node dedupCleanedupAmSeed = finalCleanedupAmSeed //
                .groupByAndLimit(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN, DataCloudConstants.AMS_ATTR_DUNS),
                        1);
        return dedupCleanedupAmSeed;
    }

    private static Node computeRootDunsAndCompare(Node amSeedFiltered, Node domOwnTableForCleanup) {
        // add ROOT_DUNS to amSeedFiltered
        ComputeRootDunsAndTypeFunction computeRootDuns = new ComputeRootDunsAndTypeFunction(
                new Fields(ROOT_DUNS, DUNS_TYPE), DataCloudConstants.ATTR_GU_DUNS,
                DataCloudConstants.ATTR_DU_DUNS, DataCloudConstants.AMS_ATTR_DUNS);
        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata(ROOT_DUNS, String.class));
        fms.add(new FieldMetadata(DUNS_TYPE, String.class));
        fieldList.add(ROOT_DUNS);
        Node amSeedWithRootDuns = amSeedFiltered //
                .apply(computeRootDuns,
                        new FieldList(DataCloudConstants.ATTR_GU_DUNS, DataCloudConstants.ATTR_DU_DUNS,
                                DataCloudConstants.AMS_ATTR_DUNS),
                        fms, new FieldList(fieldList));

        // amSeed left join domainOwnTable
        Node joinAmSeedWithDomOwnTable = amSeedWithRootDuns //
                .join(new FieldList(DataCloudConstants.AMS_ATTR_DOMAIN), domOwnTableForCleanup,
                        new FieldList(renameField(DataCloudConstants.AMS_ATTR_DOMAIN)), JoinType.LEFT);

        // function to check if ROOT_DUNS of amSeed and domOwnTable
        // are same : if equal then
        // retain and if not equal then dont retain (mark domain = null)
        CleanAmSeedWithDomOwnTabFunction cleanUpAmSeed = new CleanAmSeedWithDomOwnTabFunction(
                new Fields(joinAmSeedWithDomOwnTable.getFieldNamesArray()),
                renameField(DataCloudConstants.AMS_ATTR_DOMAIN),
                DataCloudConstants.AMS_ATTR_DUNS, DataCloudConstants.AMS_ATTR_DOMAIN, ROOT_DUNS,
                renameField(ROOT_DUNS));
        Node cleanedUpAmSeed = joinAmSeedWithDomOwnTable //
                .apply(cleanUpAmSeed,
                        new FieldList(joinAmSeedWithDomOwnTable.getFieldNames()), joinAmSeedWithDomOwnTable.getSchema(),
                        new FieldList(joinAmSeedWithDomOwnTable.getFieldNames()), Fields.REPLACE) //
                .retain(new FieldList(amSeedFiltered.getFieldNames()));

        return cleanedUpAmSeed;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }

}
