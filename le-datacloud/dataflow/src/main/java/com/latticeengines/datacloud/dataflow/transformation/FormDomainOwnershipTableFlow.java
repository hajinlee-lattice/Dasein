package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainRowSelectorAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainTreeCountAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.FormDomOwnershipTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(FormDomainOwnershipTableFlow.DATAFLOW_BEAN_NAME)
public class FormDomainOwnershipTableFlow extends ConfigurableFlowBase<FormDomOwnershipTableConfig> {
    public final static String DATAFLOW_BEAN_NAME = "FormDomainOwnershipTableFlow";
    public final static String TRANSFORMER_NAME = "FormDomOwnershipTableTransformer";
    private final static String GU_TYPE = "GU";
    private final static String DU_TYPE = "DU";
    private final static String DUNS_TYPE = "DUNS";
    private final static String ROOT_TYPE = "ROOT_TYPE";
    private final static String TREE_NUMBER = "TREE_NUMBER";
    private final static String REASON_TYPE = "REASON_TYPE";
    private final static String FRANCHISE = "FRANCHISE";

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
        Node amSeed = addSource(parameters.getBaseTables().get(0));
        Node orbSecSrc = addSource(parameters.getBaseTables().get(1));
        // filter null duns from amSeed
        String checkNullDuns = config.getAmSeedDuns() + " != null && !" + config.getAmSeedDuns() + ".equals(\"\")";
        Node amSeedDomDuns = amSeed //
                .filter(checkNullDuns, new FieldList(config.getAmSeedDuns())) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()));
        // Join orbSecSrc with amSeed on domain to get other fields info
        Node orbSecJoinedResult = orbSecSrc //
                .join(config.getOrbSecPriDom(), amSeedDomDuns, config.getAmSeedDomain(), JoinType.INNER) //
                .retain(new FieldList(config.getOrbSrcSecDom(), config.getAmSeedDuns())) //
                .rename(new FieldList(config.getOrbSrcSecDom()), new FieldList(config.getAmSeedDomain())) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()));
        // merge nodes : orbSecSrc and amSeed, then dedup the result
        Node mergedDomDuns = amSeedDomDuns //
                .merge(orbSecJoinedResult) //
                .groupByAndLimit(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()), 1);

        // join merged list of domain DUNS with amSeed
        Node mergedDomDunsWithAmSeed = mergedDomDuns //
                .join(config.getAmSeedDuns(), amSeed, config.getAmSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns(), config.getAmSeedGuDuns(),
                        config.getAmSeedDuDuns(), config.getUsSalesVolume(), config.getTotalEmp(),
                        config.getNumOfLoc()));
        Node combinedGuDuDuns = filterAndSetRootDunsType(mergedDomDunsWithAmSeed, amSeed, config);
        Node dedupedCombinedSet = combinedGuDuDuns //
                .groupByAndLimit(new FieldList(combinedGuDuDuns.getFieldNames()), 1);
        Node domOwnershipTable = constructDomOwnershipTable(dedupedCombinedSet, config);
        return domOwnershipTable;
    }

    private static Node filterAndSetRootDunsType(Node mergedDomDunsWithAmSeed, Node amSeed,
            FormDomOwnershipTableConfig config) {
        // filter null DUNS and GU_DUNS and DU_DUNS
        String chkNullDunsDuGu = "(" + config.getAmSeedDuns() + " != null && !" + config.getAmSeedDuns()
                + ".equals(\"\")) || (" + config.getAmSeedGuDuns() + " != null && !" + config.getAmSeedGuDuns()
                + ".equals(\"\")) || (" + config.getAmSeedDuDuns() + " != null && !" + config.getAmSeedDuDuns()
                + ".equals(\"\"))";
        Node filteredNull = mergedDomDunsWithAmSeed //
                .filter(chkNullDunsDuGu,
                        new FieldList(config.getAmSeedDuns(), config.getAmSeedGuDuns(), config.getAmSeedDuDuns())) //
                .groupByAndLimit(new FieldList(amSeed.getFieldNames()), 1);
        // filter function to categorize GU/DU/DUNS types
        String filterGuExp = config.getAmSeedDuns() + ".equals(" + config.getAmSeedGuDuns() + ")";
        Node filteredGuRows = filteredNull //
                .filter(filterGuExp, new FieldList(config.getAmSeedDuns(), config.getAmSeedGuDuns())) //
                .addColumnWithFixedValue(ROOT_TYPE, GU_TYPE, String.class);

        String filterDuExp = config.getAmSeedDuns() + ".equals(" + config.getAmSeedDuDuns() + ")";
        Node filteredDuRows = filteredNull //
                .filter(filterDuExp, new FieldList(config.getAmSeedDuns(), config.getAmSeedDuDuns())) //
                .addColumnWithFixedValue(ROOT_TYPE, DU_TYPE, String.class);
        String filterDunsExp = "!" + config.getAmSeedDuns() + ".equals(" + config.getAmSeedDuDuns() + ") && !"
                + config.getAmSeedDuns() + ".equals(" + config.getAmSeedGuDuns() + ")";
        Node filteredDunsRows = filteredNull //
                .filter(filterDunsExp,
                        new FieldList(config.getAmSeedDuns(), config.getAmSeedDuDuns(), config.getAmSeedGuDuns())) //
                .addColumnWithFixedValue(ROOT_TYPE, DUNS_TYPE, String.class);
        Node combinedGuDuDuns = filteredGuRows //
                .merge(filteredDuRows) //
                .merge(filteredDunsRows);
        return combinedGuDuDuns;
    }

    private static Node constructDomOwnershipTable(Node dedupedCombinedSet, FormDomOwnershipTableConfig config) {
        List<FieldMetadata> fms = fieldMetadataPrep(config);
        DomainTreeCountAggregator agg = new DomainTreeCountAggregator(
                new Fields(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_TYPE, TREE_NUMBER),
                config.getAmSeedDomain(), config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_TYPE);
        Node domainTreeNumber = dedupedCombinedSet
                .groupByAndAggregate(new FieldList(config.getAmSeedDomain()), agg, fms) //
                .renamePipe("DomainTreeNumber");
        String chkFranchiseExp = TREE_NUMBER + " >= " + config.getFranchiseThreshold();
        Node franchiseSet = domainTreeNumber //
                .filter(chkFranchiseExp, new FieldList(TREE_NUMBER)) //
                .addColumnWithFixedValue(REASON_TYPE, FRANCHISE, String.class);
        String chkTreeCount = TREE_NUMBER + " > 1 && " + TREE_NUMBER + " <= " + config.getFranchiseThreshold();
        // filtering domains present in more that one trees
        Node domainInManyTrees = domainTreeNumber //
                .filter(chkTreeCount, new FieldList(TREE_NUMBER));
        // join domainTreeNumber with dedupedCombinedSet
        Node domainRetain = domainInManyTrees //
                .rename(new FieldList(config.getAmSeedDomain()), new FieldList("renamed_" + config.getAmSeedDomain())) //
                .retain(new FieldList("renamed_" + config.getAmSeedDomain(), TREE_NUMBER));
        Node allColCombined = domainRetain //
                .join("renamed_" + config.getAmSeedDomain(), dedupedCombinedSet, config.getAmSeedDomain(),
                        JoinType.INNER);
        List<FieldMetadata> fmsForDomSelect = fieldMetadataWithReason(config);
        // aggregator to choose domain from one of the trees
        DomainRowSelectorAggregator domainSelect = new DomainRowSelectorAggregator(
                new Fields(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_TYPE, TREE_NUMBER, REASON_TYPE),
                config.getAmSeedDomain(), config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_TYPE,
                config.getUsSalesVolume(), config.getTotalEmp(), config.getNumOfLoc(), TREE_NUMBER, REASON_TYPE,
                config.getMultLargeCompThreshold());
        Node domainRowSelect = allColCombined //
                .groupByAndAggregate(new FieldList(config.getAmSeedDomain()), domainSelect, fmsForDomSelect) //
                .renamePipe("DomainRowSelect");
        Node domOwnershipTable = franchiseSet //
                .merge(domainRowSelect);
        return domOwnershipTable;
    }

    private static List<FieldMetadata> fieldMetadataWithReason(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(config.getAmSeedDomain(), String.class), //
                new FieldMetadata(config.getAmSeedDuns(), String.class), //
                new FieldMetadata(ROOT_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class), //
                new FieldMetadata(REASON_TYPE, String.class) //
        );
    }

    private static List<FieldMetadata> fieldMetadataPrep(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(config.getAmSeedDomain(), String.class), //
                new FieldMetadata(config.getAmSeedDuns(), String.class), //
                new FieldMetadata(ROOT_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class) //
        );
    }

}