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
    private final static String GU_TYPE_VAL = "GU";
    private final static String DU_TYPE_VAL = "DU";
    private final static String DUNS_TYPE_VAL = "DUNS";
    private final static String ROOT_DUNS = "ROOT_DUNS";
    private final static String DUNS_TYPE = "DUNS_TYPE";
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
        String checkNullDomain = config.getAmSeedDomain() + " != null && !" + config.getAmSeedDomain()
                + ".equals(\"\")";
        Node amSeedFilteredNull = amSeed //
                .filter(checkNullDuns, new FieldList(config.getAmSeedDuns())) //
                .filter(checkNullDomain, new FieldList(config.getAmSeedDomain()));
        Node amSeedDomDuns = amSeedFilteredNull //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns())) //
                .renamePipe("amSeedDomDuns");
        String chkNullPriDom = config.getOrbSecPriDom() + " != null && !" + config.getOrbSecPriDom() + ".equals(\"\")";
        String chkNullSecDom = config.getOrbSrcSecDom() + " != null && !" + config.getOrbSrcSecDom() + ".equals(\"\")";
        // filter null primary and secondary domains from orbSecondary
        Node orbSecSrcFiltered = orbSecSrc //
                .filter(chkNullPriDom, new FieldList(config.getOrbSecPriDom())) //
                .filter(chkNullSecDom, new FieldList(config.getOrbSrcSecDom()));
        // Join orbSecSrc with amSeed on domain to get other fields info
        Node orbSecJoinedResult = orbSecSrcFiltered //
                .join(config.getOrbSecPriDom(), amSeedDomDuns, config.getAmSeedDomain(), JoinType.INNER) //
                .retain(new FieldList(config.getOrbSrcSecDom(), config.getAmSeedDuns())) //
                .rename(new FieldList(config.getOrbSrcSecDom()), new FieldList(config.getAmSeedDomain())) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()));
        // merge nodes : orbSecSrc and amSeed, then dedup the result
        Node mergedDomDuns = amSeedDomDuns //
                .merge(orbSecJoinedResult) //
                .groupByAndLimit(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()), 1);
        // join merged list of domain DUNS with amSeed : to get required
        // columns(SalesVolume, totalEmp, numOfLocations)
        Node mergedDomDunsWithAmSeed = mergedDomDuns //
                .join(config.getAmSeedDuns(), amSeedFilteredNull, config.getAmSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns(), config.getAmSeedGuDuns(),
                        config.getAmSeedDuDuns(), config.getUsSalesVolume(), config.getTotalEmp(),
                        config.getNumOfLoc()));
        Node rootTypePopulated = filterAndSetRootDunsType(mergedDomDunsWithAmSeed, amSeed, config);
        Node domOwnershipTable = constructDomOwnershipTable(rootTypePopulated, config);
        return domOwnershipTable;
    }

    private static Node filterAndSetRootDunsType(Node mergedDomDunsWithAmSeed, Node amSeed,
            FormDomOwnershipTableConfig config) {
        // rootType selection expressions
        String chkGuExp = config.getAmSeedGuDuns() + " != null";
        String chkGuNull = config.getAmSeedGuDuns() + " == null";
        String chkDuNotNull = config.getAmSeedDuDuns() + " != null";
        String chkDuNull = config.getAmSeedDuDuns() + " == null";
        String rootDunsFirmo = config.getAmSeedDuns() + ".equals(" + ROOT_DUNS + ")";
        String filterNullDuns = ROOT_DUNS + " != null";
        // Setting RootDunsType based on selection criteria
        Node rootTypeGu = mergedDomDunsWithAmSeed //
                .apply(String.format("%s ? %s : null", chkGuExp, config.getAmSeedGuDuns()),
                        new FieldList(config.getAmSeedGuDuns()), new FieldMetadata(ROOT_DUNS, String.class)) //
                .addColumnWithFixedValue(DUNS_TYPE, GU_TYPE_VAL, String.class);

        Node rootTypeDu = mergedDomDunsWithAmSeed //
                .apply(String.format("%s ? (%s ? %s : null) : null", chkGuNull, chkDuNotNull, config.getAmSeedDuDuns()),
                        new FieldList(config.getAmSeedGuDuns(), config.getAmSeedDuDuns()),
                        new FieldMetadata(ROOT_DUNS, String.class)) //
                .addColumnWithFixedValue(DUNS_TYPE, DU_TYPE_VAL, String.class);

        Node rootTypeDuns = mergedDomDunsWithAmSeed //
                .apply(String.format("%s ? (%s ? %s : null) : null", chkDuNull, chkGuNull, config.getAmSeedDuns()),
                        new FieldList(config.getAmSeedGuDuns(), config.getAmSeedDuDuns(), config.getAmSeedDuns()),
                        new FieldMetadata(ROOT_DUNS, String.class)) //
                .addColumnWithFixedValue(DUNS_TYPE, DUNS_TYPE_VAL, String.class);

        Node rootTypePopulated = rootTypeGu //
                .merge(rootTypeDu) //
                .merge(rootTypeDuns) //
                .filter(filterNullDuns, new FieldList(ROOT_DUNS));

        Node rootOfTrees = rootTypePopulated
                .filter(rootDunsFirmo, new FieldList(config.getAmSeedDuns(), ROOT_DUNS)) //
                .retain(new FieldList(ROOT_DUNS, config.getUsSalesVolume(), config.getTotalEmp(), config.getNumOfLoc())) //
                .renamePipe("RootOfTrees");

       Node rootFirmoAdd = rootTypePopulated //
                .retain(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE) //
                .rename(new FieldList(ROOT_DUNS), new FieldList(renameField(ROOT_DUNS))) //
                .retain(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns(), renameField(ROOT_DUNS),
                        DUNS_TYPE)) //
                .join(renameField(ROOT_DUNS), rootOfTrees, ROOT_DUNS, JoinType.INNER) //
                .retain(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE,
                        config.getUsSalesVolume(), config.getTotalEmp(), config.getNumOfLoc()) //
                .groupByAndLimit(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE,
                        config.getUsSalesVolume(), config.getTotalEmp(), config.getNumOfLoc()), 1);
        return rootFirmoAdd;
    }

    private static Node constructDomOwnershipTable(Node dedupedCombinedSet, FormDomOwnershipTableConfig config) {
        List<FieldMetadata> fms = fieldMetadataPrep(config);
        DomainTreeCountAggregator agg = new DomainTreeCountAggregator(
                new Fields(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE, TREE_NUMBER),
                config.getAmSeedDomain(), config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE);
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
        // join domainTreeNumber with dedupedCombinedSet for remaining columns to be populated
        Node domainRetain = domainInManyTrees //
                .rename(new FieldList(config.getAmSeedDomain()), new FieldList(renameField(config.getAmSeedDomain()))) //
                .retain(new FieldList(renameField(config.getAmSeedDomain()), TREE_NUMBER));
        Node allColCombined = domainRetain //
                .join(renameField(config.getAmSeedDomain()), dedupedCombinedSet, config.getAmSeedDomain(),
                        JoinType.INNER);
        List<FieldMetadata> fmsForDomSelect = fieldMetadataWithReason(config);
        // aggregator to choose domain from one of the trees
        DomainRowSelectorAggregator domainSelect = new DomainRowSelectorAggregator(
                new Fields(config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE, TREE_NUMBER,
                        REASON_TYPE),
                config.getAmSeedDomain(), config.getAmSeedDomain(), config.getAmSeedDuns(), ROOT_DUNS, DUNS_TYPE,
                config.getUsSalesVolume(), config.getTotalEmp(), config.getNumOfLoc(), TREE_NUMBER, REASON_TYPE,
                config.getMultLargeCompThreshold());
        Node domainRowSelect = allColCombined //
                .groupByAndAggregate(new FieldList(config.getAmSeedDomain()), domainSelect, fmsForDomSelect) //
                .renamePipe("DomainRowSelect");
        Node domOwnershipTable = franchiseSet //
                .merge(domainRowSelect);
        return domOwnershipTable;
    }

    private static String renameField(String field) {
        return "renamed_" + field;
    }

    private static List<FieldMetadata> fieldMetadataWithReason(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(config.getAmSeedDomain(), String.class), //
                new FieldMetadata(config.getAmSeedDuns(), String.class), //
                new FieldMetadata(ROOT_DUNS, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class), //
                new FieldMetadata(REASON_TYPE, String.class) //
        );
    }

    private static List<FieldMetadata> fieldMetadataPrep(FormDomOwnershipTableConfig config) {
        return Arrays.asList(new FieldMetadata(config.getAmSeedDomain(), String.class), //
                new FieldMetadata(config.getAmSeedDuns(), String.class), //
                new FieldMetadata(ROOT_DUNS, String.class), //
                new FieldMetadata(DUNS_TYPE, String.class), //
                new FieldMetadata(TREE_NUMBER, Integer.class) //
        );
    }

}