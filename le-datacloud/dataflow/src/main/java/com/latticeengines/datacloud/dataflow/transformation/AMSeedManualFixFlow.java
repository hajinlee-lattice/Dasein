package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.am.AccountMasterBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedManualOverwriteFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ManualDomainEnrichAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(AMSeedManualFixFlow.DATAFLOW_BEAN_NAME)
public class AMSeedManualFixFlow extends AccountMasterBase<ManualSeedTransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedManualFixFlow";
    public static final String TRANSFORMER_NAME = "AMSeedManualFixTransformer";
    private static final String PRIMARY_ACCOUNT_YES = "Y";
    private static final String PRIMARY_ACCOUNT_NO = "N";
    private static final String DUNS = "DUNS";
    private static final String DU = "DU";
    private static final String LE_HQ = "LE_HQ";
    private static final String INDICATOR = "INDICATOR";
    private static final String MANUAL_DOMAIN_SOURCE = "Manual";

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
        return ManualSeedTransformerConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        ManualSeedTransformerConfig config = getTransformerConfig(parameters);
        Node amSeedDataSet = addSource(parameters.getBaseTables().get(0));
        Node manualSeedDataSet = addSource(parameters.getBaseTables().get(1));

        // filter manual seed with null duns
        String checkNullDuns = config.getManualSeedDuns() + " != null && !" + config.getManualSeedDuns()
                + ".equals(\"\")";
        manualSeedDataSet = manualSeedDataSet //
                .filter(checkNullDuns, new FieldList(config.getManualSeedDuns()));

        // prepare manual Seed fieldList
        List<String> manSeedFields = new ArrayList<>();
        manSeedFields.addAll(manualSeedDataSet.getFieldNames());
        manSeedFields.add(LE_HQ);
        manSeedFields.add(INDICATOR);

        // populate all field names
        List<String> allFieldNames = new ArrayList<>();
        allFieldNames.addAll(manualSeedDataSet.getFieldNames());
        allFieldNames.addAll(amSeedDataSet.getFieldNames());

        // populate am Seed columns
        List<String> amSeedFields = new ArrayList<>(amSeedDataSet.getFieldNames());

        // populate LE_HQ and INDICATOR fields in manual Seed
        Node manualSeedWithLeHq = populateLeHqInManSeed(config, amSeedDataSet, manualSeedDataSet, allFieldNames,
                manSeedFields);

        Node amSeedSet = overwriteAmSeed(config, manualSeedWithLeHq, amSeedDataSet, amSeedFields);

        amSeedFields.remove(LE_HQ);
        allFieldNames.remove(LE_HQ);

        // retain amSeed fields
        amSeedSet = amSeedSet //
                .retain(new FieldList(amSeedFields));

        Node enrichDomain = domainEnrich(config, amSeedSet, manualSeedDataSet, allFieldNames, amSeedFields);

        Node domainList = enrichDomain //
                .retain(new FieldList(config.getAmSeedDomain())) //
                .rename(new FieldList(config.getAmSeedDomain()), new FieldList(renameField(config.getAmSeedDomain())));

        Node filteredDomainOnly = filterDomainOnlyEntry(config, amSeedSet, domainList, amSeedFields);

        Node filteredDunsOnly = filterDunsOnlyEntry(config, filteredDomainOnly, enrichDomain, amSeedFields);

        // merge added domain node list with original list
        Node combineDomainAmSeed = filteredDunsOnly //
                .merge(enrichDomain) //
                .retain(new FieldList(amSeedFields));

        Node addedPrimaryAccount = checkPrimaryAccount(config, combineDomainAmSeed, manualSeedDataSet, allFieldNames,
                amSeedFields);

        return addedPrimaryAccount;
    }

    private Node populateLeHqInManSeed(ManualSeedTransformerConfig config, Node amSeedDataSet, Node manualSeedDataSet,
            List<String> allFieldNames, List<String> manSeedFields) {
        Node mergedDataSet = amSeedDataSet
                .join(config.getAmSeedDuns(), manualSeedDataSet, config.getManualSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(allFieldNames));

        String checkExpression = String.format("\"%s\".equals(%s) && %s != null ", "Y", config.getFixTreeFlag(),
                config.getAmSeedDuDuns());

        allFieldNames.add(LE_HQ);
        Node manualSeedWithLeHq = mergedDataSet //
                .apply(String.format("%s ? %s : %s", checkExpression, config.getAmSeedDuDuns(), config.getAmSeedDuns()),
                        new FieldList(config.getFixTreeFlag(), config.getAmSeedDuDuns(), config.getAmSeedDuns()),
                        new FieldMetadata(LE_HQ, String.class)) //
                .renamePipe("manSeedWithLeHq");
        // de-dup
        manualSeedWithLeHq = manualSeedWithLeHq //
                .groupByAndLimit(new FieldList(LE_HQ), 1);

        // adding marker indicating LE_HQ = DUNS / DU
        manualSeedWithLeHq = manualSeedWithLeHq //
                .apply(String.format("%s ? \"%s\" : \"%s\"", checkExpression, DU, DUNS),
                        new FieldList(config.getFixTreeFlag(), config.getAmSeedDuDuns()),
                        new FieldMetadata(INDICATOR, String.class)) //
                .retain(new FieldList(manSeedFields));
        return manualSeedWithLeHq;
    }

    private Node overwriteAmSeed(ManualSeedTransformerConfig config, Node manualSeedWithLeHq, Node amSeedDataSet,
            List<String> amSeedFields) {
        // conditions for filtering
        String conditionForDuns = String.format("%s.equals(\"%s\")", INDICATOR, DUNS);
        String conditionForDU = String.format("%s.equals(\"%s\")", INDICATOR, DU);

        // filter based on indicator : DU / DUNS
        Node manualSeedLeHqDuns = manualSeedWithLeHq //
                .filter(conditionForDuns, new FieldList(INDICATOR));

        Node manualSeedLeHqDU = manualSeedWithLeHq //
                .filter(conditionForDU, new FieldList(INDICATOR));

        // amSeed left join filtered parts above
        // join on DU
        Node amSeedSet = amSeedDataSet //
                .join(config.getAmSeedDuDuns(), manualSeedLeHqDU, LE_HQ, JoinType.LEFT);

        // function call to overwrite manual seed data to am seed - DU
        AMSeedManualOverwriteFunction overwriteFunction = new AMSeedManualOverwriteFunction(
                new Fields(amSeedSet.getFieldNamesArray()), LE_HQ, config.getOverwriteFields());
        amSeedSet = amSeedSet.apply(overwriteFunction, new FieldList(amSeedSet.getFieldNames()), amSeedSet.getSchema(),
                new FieldList(amSeedSet.getFieldNames()), Fields.REPLACE);

        // join on DUNS
        amSeedSet = amSeedSet //
                .retain(new FieldList(amSeedFields)) //
                .join(config.getAmSeedDuns(), manualSeedLeHqDuns, LE_HQ, JoinType.LEFT);

        // function call to overwrite manual seed data to am seed - DUNS
        amSeedSet = amSeedSet.apply(overwriteFunction, new FieldList(amSeedSet.getFieldNames()), amSeedSet.getSchema(),
                new FieldList(amSeedSet.getFieldNames()), Fields.REPLACE);
        return amSeedSet;
    }

    private Node domainEnrich(ManualSeedTransformerConfig config, Node amSeedSet, Node manualSeedDataSet,
            List<String> allFieldNames, List<String> amSeedFields) {
        // join manual Seed and am Seed by duns column
        Node enrichDomain = manualSeedDataSet //
                .join(config.getManualSeedDuns(), amSeedSet, config.getAmSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(allFieldNames));

        // call aggregator
        ManualDomainEnrichAggregator aggregator = new ManualDomainEnrichAggregator(
                new Fields(enrichDomain.getFieldNamesArray()), config.getManualSeedDomain(), config.getAmSeedDomain(),
                config.getAmSeedLeIsPrimDom(), DOMAIN_SOURCE);

        // list of domains to be added
        enrichDomain = enrichDomain //
                .groupByAndAggregate(new FieldList(config.getAmSeedDuns()), aggregator) //
                .retain(new FieldList(amSeedFields));
        return enrichDomain;
    }

    private Node filterDomainOnlyEntry(ManualSeedTransformerConfig config, Node amSeedSet, Node domainList,
            List<String> amSeedFields) {
        // domain only entries remove from amSeed - present in manual Seed
        // step1 : filter domain only entries
        Node filteredDomainOnly = amSeedSet //
                .join(config.getAmSeedDomain(), domainList, renameField(config.getAmSeedDomain()), JoinType.LEFT);

        String checkFilterCondition = String.format("(%s != null) || ((%s == null) && (%s == null))",
                config.getAmSeedDuns(), config.getAmSeedDuns(), renameField(config.getAmSeedDomain()));

        Node amSeedRetain = filteredDomainOnly //
                .filter(checkFilterCondition,
                        new FieldList(config.getAmSeedDuns(), renameField(config.getAmSeedDomain())));

        // updated amSeed data
        filteredDomainOnly = amSeedRetain.retain(new FieldList(amSeedFields));
        return filteredDomainOnly;
    }

    private Node filterDunsOnlyEntry(ManualSeedTransformerConfig config, Node filteredDomainOnly, Node enrichDomain,
            List<String> amSeedFields) {
        // step2 : filter unnecessary duns only entries
        Node dunsList = enrichDomain //
                .retain(new FieldList(config.getAmSeedDuns(), config.getAmSeedDomain())) //
                .rename(new FieldList(config.getAmSeedDuns(), config.getAmSeedDomain()),
                        new FieldList(renameField(config.getAmSeedDuns()), renameField(config.getAmSeedDomain())));

        Node filteredDunsOnly = filteredDomainOnly //
                .join(config.getAmSeedDuns(), dunsList, renameField(config.getAmSeedDuns()), JoinType.LEFT);

        String dunsFilterCondition = String.format("(%s != null) || ((%s == null) && (%s == null))",
                config.getAmSeedDomain(), config.getAmSeedDomain(), renameField(config.getAmSeedDomain()));

        // filter duns only entries
        filteredDunsOnly = filteredDunsOnly //
                .filter(dunsFilterCondition,
                        new FieldList(config.getAmSeedDomain(), renameField(config.getAmSeedDomain()))) //
                .retain(new FieldList(amSeedFields));
        return filteredDunsOnly;
    }

    private Node checkPrimaryAccount(ManualSeedTransformerConfig config, Node combineDomainAmSeed,
            Node manualSeedDataSet, List<String> allFieldNames, List<String> amSeedFields) {
        // check for primary accounts
        Node isPrimaryAccount = combineDomainAmSeed //
                .join(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()), manualSeedDataSet,
                        new FieldList(config.getManualSeedDomain(), config.getManualSeedDuns()), JoinType.LEFT);

        // add isPrimaryAccount = TRUE : if both domain/duns = not-null/empty
        String checkDomainDunsValue = String.format("%s != null && %s != null", config.getManualSeedDomain(),
                config.getManualSeedDuns());

        // check isPrimaryAccount = TRUE
        String checkIsPrimaryAcc = String.format("\"" + PRIMARY_ACCOUNT_YES + "\"" + ".equals(%s)", config.getIsPrimaryAccount());

        amSeedFields.add(config.getIsPrimaryAccount());
        Node addedPrimaryAccount = isPrimaryAccount //
                .apply(String.format("%s ? \"%s\" : \"%s\"", checkDomainDunsValue, PRIMARY_ACCOUNT_YES,
                        PRIMARY_ACCOUNT_NO), new FieldList(config.getManualSeedDomain(), config.getManualSeedDuns()),
                        new FieldMetadata(config.getIsPrimaryAccount(), String.class)) //
                .apply(String.format("%s ? \"%s\" : %s", checkIsPrimaryAcc, MANUAL_DOMAIN_SOURCE, DOMAIN_SOURCE),
                        new FieldList(config.getIsPrimaryAccount(), DOMAIN_SOURCE),
                        new FieldMetadata("NewDomainSource", String.class))
                .discard(new FieldList(DOMAIN_SOURCE)) //
                .rename(new FieldList("NewDomainSource"), new FieldList(DOMAIN_SOURCE)) //
                .retain(new FieldList(amSeedFields));
        return addedPrimaryAccount;

    }

    private String renameField(String field) {
        return "RENAMED_" + field;
    }

}
