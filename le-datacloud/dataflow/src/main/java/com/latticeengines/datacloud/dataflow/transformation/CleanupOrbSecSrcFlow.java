package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(CleanupOrbSecSrcFlow.DATAFLOW_BEAN_NAME)
public class CleanupOrbSecSrcFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "CleanupOrbSecSrcFlow";
    public final static String TRANSFORMER_NAME = "CleanupOrbSecSrcTransformer";
    private final static String DUNS_TYPE = "DUNS_TYPE";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String FRANCHISE = "FRANCHISE";
    private final static String MULTIPLE_LARGE_COMPANY = "MULTIPLE_LARGE_COMPANY";
    private final static String GU_TYPE_VAL = "GU";
    private final static String DU_TYPE_VAL = "DU";
    private final static String DUNS_TYPE_VAL = "DUNS";
    private final static String ROOT_DUNS = "ROOT_DUNS";

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
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        Node amSeed = addSource(parameters.getBaseTables().get(2));

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
                .filter(filterDomOwnTable, new FieldList(REASON_TYPE)); //
        Node retainedOrbSecSrc = recordsRetainedFromOrb(config, amSeedFiltered, orbSecSrc, domOwnTableFilter);

        String filterNullDomOwnTabDom = config.getAmSeedDomain() + " == null";
        Node nonMatchOrbSecSrc = orbSecSrc //
                .join(config.getOrbSrcSecDom(), domOwnershipTable, config.getAmSeedDomain(), JoinType.LEFT) //
                .filter(filterNullDomOwnTabDom, new FieldList(config.getAmSeedDomain())) //
                .retain(new FieldList(config.getOrbSecPriDom(), config.getOrbSrcSecDom()));
        Node orbSecSrcCleanedUp = retainedOrbSecSrc //
                .merge(nonMatchOrbSecSrc);
        return orbSecSrcCleanedUp;
    }

    private static Node recordsRetainedFromOrb(FormDomOwnershipTableConfig config, Node amSeedFiltered, Node orbSecSrc,
            Node domOwnTableFilter) {
        // join OrbSecSrc with amSeed
        Node orbSecSrcJoinAmSeed = orbSecSrc //
                .join(config.getOrbSecPriDom(), amSeedFiltered, config.getAmSeedDomain(), JoinType.INNER) //
                .retain(new FieldList(config.getOrbSecPriDom(), config.getOrbSrcSecDom(), config.getAmSeedGuDuns(),
                        config.getAmSeedDuDuns(), config.getAmSeedDuns()));
        Node retainedOrbSecSrc = null;
        if (orbSecSrcJoinAmSeed != null) {
            // join OrbSecSrcJoinAmSeed with domOwnTable
            Node orbSecSrcJoinAmSeedJoinDomOwnTab = orbSecSrcJoinAmSeed //
                    .join(new FieldList(config.getOrbSrcSecDom()), domOwnTableFilter,
                            new FieldList(config.getAmSeedDomain()), JoinType.INNER);
            // check Gu
            String filterGuExp = DUNS_TYPE + ".equals(\"" + GU_TYPE_VAL + "\")";
            String chkGuValExpMatch = ROOT_DUNS + ".equals(" + config.getAmSeedGuDuns() + ")";
            // partition Domain ownership table as per GU
            Node orbSecSrcGuSet = orbSecSrcJoinAmSeedJoinDomOwnTab //
                    .filter(filterGuExp, new FieldList(DUNS_TYPE));
            Node orbSecSrcRetainedGu = orbSecSrcGuSet //
                    .filter(chkGuValExpMatch, new FieldList(ROOT_DUNS, config.getAmSeedGuDuns())) //
                    .retain(new FieldList(config.getOrbSecPriDom(), config.getOrbSrcSecDom()));

            // check Du
            String filterDuExp = DUNS_TYPE + ".equals(\"" + DU_TYPE_VAL + "\")";
            String chkDuValExpMatch = ROOT_DUNS + ".equals(" + config.getAmSeedDuDuns() + ")";
            // partition Domain ownership table as per DU
            Node orbSecSrcDuSet = orbSecSrcJoinAmSeedJoinDomOwnTab //
                    .filter(filterDuExp, new FieldList(DUNS_TYPE));
            Node orbSecSrcRetainedDu = orbSecSrcDuSet //
                    .filter(chkDuValExpMatch, new FieldList(ROOT_DUNS, config.getAmSeedDuDuns())) //
                    .retain(new FieldList(config.getOrbSecPriDom(), config.getOrbSrcSecDom()));

            // check Duns
            String filterDunsExp = DUNS_TYPE + ".equals(\"" + DUNS_TYPE_VAL + "\")";
            String chkDunsValExpMatch = ROOT_DUNS + ".equals(" + config.getAmSeedDuns() + ")";
            // partition Domain ownership table as per duns
            Node orbSecSrcDunsSet = orbSecSrcJoinAmSeedJoinDomOwnTab //
                    .filter(filterDunsExp, new FieldList(DUNS_TYPE));
            Node orbSecSrcRetainedDuns = orbSecSrcDunsSet //
                    .filter(chkDunsValExpMatch, new FieldList(ROOT_DUNS, config.getAmSeedDuns())) //
                    .retain(new FieldList(config.getOrbSecPriDom(), config.getOrbSrcSecDom()));

            retainedOrbSecSrc = orbSecSrcRetainedGu //
                    .merge(orbSecSrcRetainedDu) //
                    .merge(orbSecSrcRetainedDuns);
        }
        return retainedOrbSecSrc;
    }
}
