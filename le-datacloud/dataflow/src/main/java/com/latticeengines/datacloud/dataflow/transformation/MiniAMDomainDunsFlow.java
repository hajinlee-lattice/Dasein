package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MiniAMDomainDunsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("miniAMDomainDunsFlow")
public class MiniAMDomainDunsFlow extends ConfigurableFlowBase<MiniAMDomainDunsConfig> {
    private static final Logger log = LoggerFactory.getLogger(MiniAMDomainDunsFlow.class);
    private final static String DNB_GU_VALUE = "GuDnbValue";
    private final static String DNB_DU_VALUE = "DuDnbValue";
    private final static String DOMAIN_TYPE = "Domain";
    private final static String DUNS_TYPE = "Duns";
    @Override
    public Node construct(TransformationFlowParameters parameters) {
        List<String> miniTargetTableAttr = getMiniTargetTableAttr(parameters);
        MiniAMDomainDunsConfig config = getTransformerConfig(parameters);
        Node miniDomainDunsList = addSource(parameters.getBaseTables().get(0));
        Node dnbSeedDataSet = addSource(parameters.getBaseTables().get(1));
        // Renaming the source name to pipeName to ensure they are consistent
        dnbSeedDataSet.renamePipe(dnbSeedDataSet.getPipeName());
        List<Node> storeList = new ArrayList<Node>();
        Map<String, String> seedsDomain = config.getSeedInputDataSetDomain();
        Map<String, String> seedsDuns = config.getSeedInputDataSetDuns();
        storeList.add(dnbSeedDataSet);
        for (int i = 2; i < parameters.getBaseTables().size(); i++) {
            Node seed = addSource(parameters.getBaseTables().get(i));
            // Renaming source name to pipeName to ensure they are consistent
            seed.renamePipe(seed.getPipeName());
            storeList.add(addSource(parameters.getBaseTables().get(i)));
        }

        // Adding DUNS by matching with domains
        List<Node> storeDunsValues = new ArrayList<Node>();
        for (Node seedDataSet : storeList) {
            log.info("domain : " + seedsDomain.get(seedDataSet.getPipeName()) + "duns : "
                    + seedsDuns.get(seedDataSet.getPipeName()));
            Node dunsMatchedByDomain = miniDomainDunsList
                    .join(config.getOutputDataSetValue(), seedDataSet, seedsDomain.get(seedDataSet.getPipeName()),
                            JoinType.INNER) //
                    .retain(seedsDuns.get(seedDataSet.getPipeName())) //
                    .rename(new FieldList(seedsDuns.get(seedDataSet.getPipeName())),
                            new FieldList(config.getOutputDataSetValue()))
                    .addColumnWithFixedValue(config.getOutputDataSetType(), DUNS_TYPE,
                            String.class) //
                    .retain(new FieldList(miniDomainDunsList.getFieldNames())) //
                    .renamePipe(seedDataSet + "duns");
            storeDunsValues.add(dunsMatchedByDomain);
        }

        // Merge into target table
        miniDomainDunsList = miniDomainDunsList.merge(storeDunsValues);
        // Remove Nulls
        String checkNullExpression = config.getOutputDataSetValue() + " != null  && " + "!"
                + config.getOutputDataSetValue() + ".equals(\"\")";
        miniDomainDunsList = miniDomainDunsList.filter(checkNullExpression,
                new FieldList(config.getOutputDataSetValue()));
        // De-duplication
        miniDomainDunsList = miniDomainDunsList.groupByAndLimit(new FieldList(miniTargetTableAttr), 1);
        /*
         * Adding DUNS from DnbDataSeed by matching with GU, DU hierarchy and
         * storing the result into Node (Type{GU, DU}, Value{DUNS associated to
         * GU, DU hierarchy})
         */
        String checkGuExpression = config.getDnbInputDataSetGU() + " != null";
        String checkDuExpression = config.getDnbInputDataSetGU() + " == null";
        Node storeDnbHierarchy = miniDomainDunsList
                .join(config.getOutputDataSetValue(), dnbSeedDataSet, config.getDnbInputDataSetDuns(), JoinType.INNER)
                .renamePipe("storeDnbGuDu");

        // Dnb GU data set node
        Node storeDnbGu = storeDnbHierarchy.retain(config.getDnbInputDataSetGU()) //
                .apply(String.format("%s ? %s : null", checkGuExpression,
                        config.getDnbInputDataSetGU()), new FieldList(config.getDnbInputDataSetGU()),
                        new FieldMetadata(DNB_GU_VALUE, String.class)) //
                .retain(DNB_GU_VALUE) //
                .renamePipe("storeDnbGu");

        // Removing nulls from Dnb GU data set node
        String checkNullGuDuns = DNB_GU_VALUE + " != null ";
        storeDnbGu = storeDnbGu.filter(checkNullGuDuns, new FieldList(DNB_GU_VALUE));

        // De-duplication
        storeDnbGu = storeDnbGu.groupByAndLimit(new FieldList(DNB_GU_VALUE), 1);

        /*
         * Update DU DataSet : Left Join DU dataSet with GU DataSet so that
         * hierarchy is maintained (whatever DUNS exist under DU, if same one
         * exist in GU then DU ones should be discarded, since GU takes
         * precedence over DU)
         */
        Node storeDnbDu = storeDnbHierarchy.retain(config.getDnbInputDataSetGU(), config.getDnbInputDataSetDU()) //
                .apply(String.format("%s ? %s : null", checkDuExpression, config.getDnbInputDataSetDU()), //
                        new FieldList(config.getDnbInputDataSetGU(), config.getDnbInputDataSetDU()),
                        new FieldMetadata(DNB_DU_VALUE, String.class)) //
                .retain(DNB_DU_VALUE) //
                .renamePipe("storeDnbDu");

        // Removing nulls from Dnb DU data set node
        String checkNullDuDuns = DNB_DU_VALUE + " != null ";
        storeDnbDu = storeDnbDu.filter(checkNullDuDuns, new FieldList(DNB_DU_VALUE));

        // Deduplication
        storeDnbDu = storeDnbDu.groupByAndLimit(new FieldList(DNB_DU_VALUE), 1);

        // Removing DUNS from DU which are already present in GU
        String checkValInGU = DNB_GU_VALUE + " == null";
        storeDnbDu = storeDnbDu.join(DNB_DU_VALUE, storeDnbGu, DNB_GU_VALUE, JoinType.LEFT) //
                .filter(checkValInGU, new FieldList(DNB_GU_VALUE, DNB_DU_VALUE));
        /*
         * Joining the DU and GU nodes DunsValue with Dnb Seed to get the
         * respective DUNS under DU and GU
         */
        List<Node> dunsFromGuDu = new ArrayList<Node>();
        // DUNS from GuNode
        Node guDuns = storeDnbGu.join(DNB_GU_VALUE, dnbSeedDataSet, config.getDnbInputDataSetGU(), JoinType.INNER) //
                .retain(config.getDnbInputDataSetDuns()) //
                .rename(new FieldList(config.getDnbInputDataSetDuns()), new FieldList(config.getOutputDataSetValue())) //
                .addColumnWithFixedValue(config.getOutputDataSetType(), DUNS_TYPE, String.class) //
                .retain(new FieldList(miniDomainDunsList.getFieldNames())) //
                .renamePipe("GuDuns");
        dunsFromGuDu.add(guDuns);

        // DUNS from DuNode
        Node duDuns = storeDnbDu.join(DNB_DU_VALUE, dnbSeedDataSet, config.getDnbInputDataSetDU(), JoinType.INNER) //
                .retain(config.getDnbInputDataSetDuns()) //
                .rename(new FieldList(config.getDnbInputDataSetDuns()), new FieldList(config.getOutputDataSetValue())) //
                .addColumnWithFixedValue(config.getOutputDataSetType(), DUNS_TYPE, String.class) //
                .retain(new FieldList(miniDomainDunsList.getFieldNames())) //
                .renamePipe("DuDuns");
        dunsFromGuDu.add(duDuns);

        // Merge into target table
        miniDomainDunsList = miniDomainDunsList.merge(dunsFromGuDu);

        // Remove Nulls
        miniDomainDunsList = miniDomainDunsList.filter(checkNullExpression,
                new FieldList(config.getOutputDataSetValue()));
        // De-duplication
        miniDomainDunsList = miniDomainDunsList.groupByAndLimit(new FieldList(miniTargetTableAttr), 1);

        // Adding domains by matching with DUNS
        List<Node> storeDomainValues = new ArrayList<Node>();
        for (Node seedDataSet : storeList) {
            Node domainsMatchedByDuns = miniDomainDunsList
                    .join(config.getOutputDataSetValue(), seedDataSet, seedsDuns.get(seedDataSet.getPipeName()), JoinType.INNER) //
                    .retain(seedsDomain.get(seedDataSet.getPipeName())) //
                    .rename(new FieldList(seedsDomain.get(seedDataSet.getPipeName())),
                            new FieldList(config.getOutputDataSetValue())) //
                    .addColumnWithFixedValue(config.getOutputDataSetType(), DOMAIN_TYPE,
                            String.class) //
                    .retain(new FieldList(miniDomainDunsList.getFieldNames())) //
                    .renamePipe(seedDataSet + "domain");
            storeDomainValues.add(domainsMatchedByDuns);
        }

        // Merge into target table
        miniDomainDunsList = miniDomainDunsList.merge(storeDomainValues);
        // Remove Nulls
        miniDomainDunsList = miniDomainDunsList.filter(checkNullExpression,
                new FieldList(config.getOutputDataSetValue()));
        // De-duplication
        miniDomainDunsList = miniDomainDunsList.groupByAndLimit(new FieldList(miniTargetTableAttr), 1);

        return miniDomainDunsList;
    }

    private List<String> getMiniTargetTableAttr(TransformationFlowParameters parameters) {
        List<String> miniTargetTableAttr = new ArrayList<String>();
        MiniAMDomainDunsConfig config = getTransformerConfig(parameters);
        miniTargetTableAttr.add(config.getOutputDataSetType());
        miniTargetTableAttr.add(config.getOutputDataSetValue());
        return miniTargetTableAttr;
    }

    @Override
    public String getDataFlowBeanName() {
        return "miniAMDomainDunsFlow";
    }

    @Override
    public String getTransformerName() {
        return "miniAMDomainDunsTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return MiniAMDomainDunsConfig.class;
    }

}
