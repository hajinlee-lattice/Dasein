package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(CleanupAmSeedSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupAmSeedSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupAmSeedSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupAmSeedSrcTransformer";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String FRANCHISE = "FRANCHISE";
    private final static String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String GU_TYPE_VAL = "GU";
    private final static String DU_TYPE_VAL = "DU";
    private final static String DUNS_TYPE_VAL = "DUNS";
    private final static String IS_RETAINED = "IS_RETAINED";
    private final static String RETAIN_YES = "YES";
    private final static String RETAIN_NO = "NO";
    private final static String NULL_DOMAIN = "NULL_DOMAIN";

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
        FormDomOwnershipTableConfig config = getTransformerConfig(parameters);
        Node domOwnershipTable = addSource(parameters.getBaseTables().get(0));
        Node amSeed = addSource(parameters.getBaseTables().get(1));
        // filter null duns from amSeed
        String checkNullDuns = config.getAmSeedDuns() + " != null && !" + config.getAmSeedDuns() + ".equals(\"\")";
        String checkNullDomain = config.getAmSeedDomain() + " != null && !" + config.getAmSeedDomain()
                + ".equals(\"\")";
        Node amSeedFiltered = amSeed //
                .filter(checkNullDuns, new FieldList(config.getAmSeedDuns())) //
                .filter(checkNullDomain, new FieldList(config.getAmSeedDomain()));
        String filterDomOwnTable = "!" + REASON_TYPE + ".equals(\"" + FRANCHISE + "\") && !" + REASON_TYPE
                + ".equals(\"" + MULTIPLE_LARGE_COMPANY + "\")";
        // retaining required columns : domain, root duns, dunsType
        Node domOwnTableFilter = domOwnershipTable //
                .filter(filterDomOwnTable, new FieldList(REASON_TYPE)) //
                .rename(new FieldList(config.getAmSeedDomain()), new FieldList(renameField(config.getAmSeedDomain()))) //
                .retain(new FieldList(renameField(config.getAmSeedDomain()), ROOT_DUNS, DUNS_TYPE));
        // amSeed Cleanup join
        Node joinAmSeedWithDomOwnTable = amSeedFiltered //
                .join(config.getAmSeedDomain(), domOwnTableFilter, renameField(config.getAmSeedDomain()),
                        JoinType.LEFT);

        // filter records not in domain ownership table
        String filterNullDom = renameField(config.getAmSeedDomain()) + " == null";
        Node recordsNotInDomOwnTable = joinAmSeedWithDomOwnTable //
                .filter(filterNullDom, new FieldList(renameField(config.getAmSeedDomain()))) //
                .retain(new FieldList(amSeedFiltered.getFieldNames()));
        String filterNotNullDom = renameField(config.getAmSeedDomain()) + " != null";
        Node recordsInDomOwnTable = joinAmSeedWithDomOwnTable //
                .filter(filterNotNullDom, new FieldList(renameField(config.getAmSeedDomain())));

        Node retainedGu = filterGuAmSeed(config, recordsInDomOwnTable);
        Node retainedDu = filterDuAmSeed(config, recordsInDomOwnTable);
        Node retainedDuns = filterDunsAmSeed(config, recordsInDomOwnTable);

        Node amSeedWithRetain = retainedGu //
                .merge(retainedDu) //
                .merge(retainedDuns);

        String filterRetainPresent = IS_RETAINED + ".equals(\"" + RETAIN_YES + "\")";
        String filterNotRetain = IS_RETAINED + ".equals(\"" + RETAIN_NO + "\")";
        Node filterNotRetained = null;
        Node filterRetained = null;
        if (amSeedWithRetain != null) {
            filterNotRetained = amSeedWithRetain //
                    .filter(filterNotRetain, new FieldList(IS_RETAINED)) //
                    .addColumnWithFixedValue(NULL_DOMAIN, null, String.class) //
                    .rename(new FieldList(config.getAmSeedDomain(), NULL_DOMAIN),
                            new FieldList(NULL_DOMAIN, config.getAmSeedDomain())) //
                    .retain(new FieldList(amSeedFiltered.getFieldNames()));

            filterRetained = amSeedWithRetain //
                    .filter(filterRetainPresent, new FieldList(IS_RETAINED)) //
                    .retain(new FieldList(amSeedFiltered.getFieldNames()));
        }

        Node finalCleanedupAmSeed = filterNotRetained //
                .merge(filterRetained) //
                .merge(recordsNotInDomOwnTable);

        String filterNullDomain = config.getAmSeedDomain() + " == null && " + config.getAmSeedDuns() + " != null";
        String filterDomDunsExist = config.getAmSeedDomain() + " != null && " + config.getAmSeedDuns() + " != null";
        // remove duns only entries if another record exists with the value of
        // same duns
        Node cleanedUpAmSeedWithNullDom = finalCleanedupAmSeed //
                .filter(filterNullDomain, new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns())) //
                .renamePipe("cleanedUpAmSeedWithNullDom");
        Node cleanedUpAmSeedWithDom = finalCleanedupAmSeed //
                .filter(filterDomDunsExist, new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns())) //
                .renamePipe("cleanedUpAmSeedWithDom");
        Node dunsOnlyEntriesToCleanup = cleanedUpAmSeedWithNullDom //
                .join(config.getAmSeedDuns(), cleanedUpAmSeedWithDom, config.getAmSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(cleanedUpAmSeedWithNullDom.getFieldNames())) //
                .rename(new FieldList(config.getAmSeedDuns()), new FieldList(renameField(config.getAmSeedDuns()))) //
                .retain(new FieldList(renameField(config.getAmSeedDuns())));
        if (dunsOnlyEntriesToCleanup != null) {
            String filterRecordsToCleanup = renameField(config.getAmSeedDuns()) + " == null";
            finalCleanedupAmSeed = finalCleanedupAmSeed //
                    .join(config.getAmSeedDuns(), dunsOnlyEntriesToCleanup, renameField(config.getAmSeedDuns()),
                            JoinType.LEFT) //
                    .filter(filterRecordsToCleanup, new FieldList(renameField(config.getAmSeedDuns()))) //
                    .retain(new FieldList(finalCleanedupAmSeed.getFieldNames()));
        }
        return finalCleanedupAmSeed;
    }

    private static Node filterGuAmSeed(FormDomOwnershipTableConfig config, Node recordsInDomOwnTable) {
        // filter GU
        String filterGuExp = DUNS_TYPE + ".equals(\"" + GU_TYPE_VAL + "\")";
        String filterNullGu = config.getAmSeedGuDuns() + " != null";
        Node filterGuFromJoinedResult = recordsInDomOwnTable //
                .filter(filterGuExp, new FieldList(DUNS_TYPE)) //
                .filter(filterNullGu, new FieldList(config.getAmSeedGuDuns()));
        Node retainedGu = null;
        if (filterGuFromJoinedResult != null) {
            String guExpRetain = config.getAmSeedGuDuns() + ".equals(" + ROOT_DUNS + ")";
            String guExpNonRetain = "!" + config.getAmSeedGuDuns() + ".equals(" + ROOT_DUNS + ")";
            // apply filter to check if GU and rootType value are same
            Node filterRetainedFromGu = filterGuFromJoinedResult //
                    .filter(guExpRetain, new FieldList(config.getAmSeedGuDuns(), ROOT_DUNS));

            if (filterRetainedFromGu != null)
                filterRetainedFromGu = filterRetainedFromGu //
                        .addColumnWithFixedValue(IS_RETAINED, RETAIN_YES, String.class);
            Node filterNonRetainedFromGu = filterGuFromJoinedResult //
                    .filter(guExpNonRetain, new FieldList(config.getAmSeedGuDuns(), ROOT_DUNS)); //
            if (filterNonRetainedFromGu != null)
                filterNonRetainedFromGu = filterNonRetainedFromGu.addColumnWithFixedValue(IS_RETAINED, RETAIN_NO,
                        String.class);

            retainedGu = filterRetainedFromGu //
                    .merge(filterNonRetainedFromGu);
        }
        return retainedGu;
    }

    private static Node filterDuAmSeed(FormDomOwnershipTableConfig config, Node recordsInDomOwnTable) {
        // filter DU
        String filterDuExp = DUNS_TYPE + ".equals(\"" + DU_TYPE_VAL + "\")";
        String filterNullDu = config.getAmSeedDuDuns() + " != null";
        Node filterDuFromJoinedResult = recordsInDomOwnTable //
                .filter(filterDuExp, new FieldList(DUNS_TYPE)) //
                .filter(filterNullDu, new FieldList(config.getAmSeedDuDuns()));

        Node retainedDu = null;
        if (filterDuFromJoinedResult != null) {
            String duExpRetain = config.getAmSeedDuDuns() + ".equals(" + ROOT_DUNS + ")";
            String duExpNonRetain = "!" + config.getAmSeedDuDuns() + ".equals(" + ROOT_DUNS + ")";
            // apply filter to check if DU and rootType value are same
            Node filterRetainedFromDu = filterDuFromJoinedResult //
                    .filter(duExpRetain, new FieldList(config.getAmSeedDuDuns(), ROOT_DUNS));
            if (filterRetainedFromDu != null)
                filterRetainedFromDu = filterRetainedFromDu //
                    .addColumnWithFixedValue(IS_RETAINED, RETAIN_YES, String.class);
            Node filterNonRetainedFromDu = filterDuFromJoinedResult //
                    .filter(duExpNonRetain, new FieldList(config.getAmSeedDuDuns(), ROOT_DUNS));
            if (filterNonRetainedFromDu != null)
                filterNonRetainedFromDu = filterNonRetainedFromDu.addColumnWithFixedValue(IS_RETAINED, RETAIN_NO,
                        String.class);
            retainedDu = filterRetainedFromDu //
                    .merge(filterNonRetainedFromDu);
        }
        return retainedDu;
    }

    private static Node filterDunsAmSeed(FormDomOwnershipTableConfig config, Node recordsInDomOwnTable) {
        // filter DUNS
        String filterDunsExp = DUNS_TYPE + ".equals(\"" + DUNS_TYPE_VAL + "\")";
        String filterNullDuns = config.getAmSeedDuns() + " != null";
        Node filterDunsFromJoinedResult = recordsInDomOwnTable //
                .filter(filterDunsExp, new FieldList(DUNS_TYPE)) //
                .filter(filterNullDuns, new FieldList(config.getAmSeedDuns()));

        Node retainedDuns = null;
        if(filterDunsFromJoinedResult != null) {
            String dunsExpRetain = config.getAmSeedDuns() + ".equals(" + ROOT_DUNS + ")";
            String dunsExpNonRetain = "!" + config.getAmSeedDuns() + ".equals(" + ROOT_DUNS + ")";
            // apply filter to check if DUNS and rootType value are same
            Node filterRetainedFromDuns = filterDunsFromJoinedResult //
                    .filter(dunsExpRetain, new FieldList(config.getAmSeedDuns(), ROOT_DUNS));
            if(filterRetainedFromDuns != null)
                filterRetainedFromDuns = filterRetainedFromDuns //
                    .addColumnWithFixedValue(IS_RETAINED, RETAIN_YES, String.class);
            Node filterNonRetainedFromDuns = filterDunsFromJoinedResult //
                    .filter(dunsExpNonRetain, new FieldList(config.getAmSeedDuns(), ROOT_DUNS));
            if(filterNonRetainedFromDuns != null)
                filterNonRetainedFromDuns = filterNonRetainedFromDuns //
                    .addColumnWithFixedValue(IS_RETAINED, RETAIN_NO, String.class);
            retainedDuns = filterRetainedFromDuns //
                    .merge(filterNonRetainedFromDuns);
        }
        return retainedDuns;

    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }

}
