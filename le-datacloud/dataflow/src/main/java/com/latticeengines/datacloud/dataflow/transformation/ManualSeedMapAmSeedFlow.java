package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedManualOverwriteFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AmSeedOverwriteDomainOnly;
import com.latticeengines.dataflow.runtime.cascading.propdata.ManualDomainEnrichAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ManualSeedTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(ManualSeedMapAmSeedFlow.DATAFLOW_BEAN_NAME)
public class ManualSeedMapAmSeedFlow extends ConfigurableFlowBase<ManualSeedTransformerConfig> {

    public final static String DATAFLOW_BEAN_NAME = "AMSeedManualFixFlow";
    public final static String TRANSFORMER_NAME = "AMSeedManualFixTransformer";
    private final static String PRIMARY_ACCOUNT_YES = "Y";
    private final static String PRIMARY_ACCOUNT_NO = "N";
    private final static String DUNS = "DUNS";
    private final static String DU = "DU";
    private final static String LE_HQ = "LE_HQ";
    private final static String INDICATOR = "INDICATOR";
    public final static String MANUAL_SEED_LE_HQ = "MANUAL_SEED_LE_HQ";

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
        Node manualSeedDataSet = addSource(parameters.getBaseTables().get(0));
        Node amSeedDataSet = addSource(parameters.getBaseTables().get(1));

        // filter manual seed with null duns
        String checkNullDuns = config.getManualSeedDuns() + " != null && !" + config.getManualSeedDuns()
                + ".equals(\"\")";
        manualSeedDataSet = manualSeedDataSet //
                .filter(checkNullDuns, new FieldList(config.getManualSeedDuns()));

        // prepare manual Seed fieldList
        List<String> manSeedFields = new ArrayList<String>();
        manSeedFields.addAll(manualSeedDataSet.getFieldNames());
        manSeedFields.add(LE_HQ);

        // populate all field names
        List<String> allFieldNames = new ArrayList<String>();
        allFieldNames.addAll(manualSeedDataSet.getFieldNames());
        allFieldNames.addAll(amSeedDataSet.getFieldNames());

        // populate am Seed columns
        List<String> amSeedFields = new ArrayList<String>();
        amSeedFields.addAll(amSeedDataSet.getFieldNames());

        // populate LE_HQ in manual Seed
        Node mergedDataSet = amSeedDataSet
                .join(config.getAmSeedDuns(), manualSeedDataSet, config.getManualSeedDuns(),
                        JoinType.INNER) //
                .retain(new FieldList(allFieldNames));

        String value = "Y";
        String checkExpression = String.format("\"%s\".equals(%s) && %s != null ", value, config.getFixTreeFlag(),
                config.getAmSeedDuDuns());

        allFieldNames.add(LE_HQ);
        Node manualSeedWithLeHq = mergedDataSet //
                .apply(String.format("%s ? %s : %s", checkExpression, config.getAmSeedDuDuns(), config.getAmSeedDuns()),
                        new FieldList(config.getFixTreeFlag(), config.getAmSeedDuDuns(),
                                config.getAmSeedDuns()),
                        new FieldMetadata(LE_HQ, String.class)) //
                .renamePipe("manSeedWithLeHq");

        // de-dup
        manualSeedWithLeHq = manualSeedWithLeHq //
                .groupByAndLimit(new FieldList(LE_HQ), 1);

        manSeedFields.add(INDICATOR);
        // adding marker indicating LE_HQ = DUNS / DU
        manualSeedWithLeHq = manualSeedWithLeHq //
                .apply(String.format("%s ? \"%s\" : \"%s\"", checkExpression, DU, DUNS),
                        new FieldList(config.getFixTreeFlag(), config.getAmSeedDuDuns()),
                        new FieldMetadata(INDICATOR, String.class)) //
                .retain(new FieldList(manSeedFields));

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
        AMSeedManualOverwriteFunction overwriteFunction1 = new AMSeedManualOverwriteFunction(
                new Fields(amSeedSet.getFieldNamesArray()), config.getAmSeedSalesVolume(), config.getAmSeedTotalEmp(),
                config.getManualSeedSalesVolume(), config.getManualSeedTotalEmp(),
                LE_HQ);

        amSeedSet = amSeedSet.apply(overwriteFunction1, new FieldList(amSeedSet.getFieldNames()), amSeedSet.getSchema(),
                new FieldList(amSeedSet.getFieldNames()), Fields.REPLACE);
        
        // join on DUNS
        amSeedSet = amSeedSet //
                .join(config.getAmSeedDuns(), manualSeedLeHqDuns, LE_HQ, JoinType.LEFT) //
                .retain(new FieldList(amSeedSet.getFieldNames()));

        // function call to overwrite manual seed data to am seed - DUNS
        amSeedSet = amSeedSet.apply(overwriteFunction1, new FieldList(amSeedSet.getFieldNames()), amSeedSet.getSchema(),
                new FieldList(amSeedSet.getFieldNames()), Fields.REPLACE);

        amSeedFields.remove(LE_HQ);
        allFieldNames.remove(LE_HQ);

        // retain amSeed fields
        amSeedSet = amSeedSet //
                .retain(new FieldList(amSeedFields));

        // join manual Seed and am Seed by duns column
        Node enrichDomain = manualSeedDataSet //
                .join(config.getManualSeedDuns(), amSeedSet, config.getAmSeedDuns(), JoinType.INNER) //
                .retain(new FieldList(allFieldNames));

        // call aggregator
        ManualDomainEnrichAggregator aggregator = new ManualDomainEnrichAggregator(new Fields(enrichDomain.getFieldNamesArray()),
                config.getManualSeedDomain(), config.getAmSeedDomain(),
                config.getAmSeedLeIsPrimDom());

        // list of domains to be added
        enrichDomain = enrichDomain //
                .groupByAndAggregate(new FieldList(config.getAmSeedDuns()), aggregator) //
                .retain(new FieldList(amSeedFields));

        // domain only entries remove from amSeed - present in manual Seed
        // step1 : filter domain only entries
        String checkDomainOnly = config.getAmSeedDuns() + " == null";
        Node domainOnlyEntries = enrichDomain //
                .filter(checkDomainOnly, new FieldList(config.getAmSeedDuns()));

        String checkDomainAndDuns = config.getAmSeedDuns() + " != null";
        Node domainAndDuns = enrichDomain //
                .filter(checkDomainAndDuns, new FieldList(config.getAmSeedDuns()));

        // join manual seed with domain only entries in am seed
        Node filterEnrichedDom = domainOnlyEntries //
                .join(config.getAmSeedDomain(), manualSeedDataSet, config.getManualSeedDomain(), JoinType.LEFT) //
                .retain(new FieldList(amSeedFields));

        // check corresponding manual seed value and overwrite into amSeed
        AmSeedOverwriteDomainOnly overwriteFunction3 = new AmSeedOverwriteDomainOnly(
                new Fields(filterEnrichedDom.getFieldNamesArray()), config.getManualSeedDuns(), config.getAmSeedDuns());

        filterEnrichedDom = filterEnrichedDom.apply(overwriteFunction3,
                new FieldList(filterEnrichedDom.getFieldNames()),
                filterEnrichedDom.getSchema(), new FieldList(filterEnrichedDom.getFieldNames()), Fields.REPLACE);

        // filtered domain Only entries
        enrichDomain = domainAndDuns //
                .merge(filterEnrichedDom);

        // merge added domain node list with original list
        Node combineDomainAmSeed = amSeedSet //
                .merge(enrichDomain) //
                .retain(new FieldList(amSeedFields));

        // check for primary accounts
        Node isPrimaryAccount = combineDomainAmSeed //
                .join(new FieldList(config.getAmSeedDomain(), config.getAmSeedDuns()), manualSeedDataSet,
                        new FieldList(config.getManualSeedDomain(), config.getManualSeedDuns()),
                        JoinType.LEFT) //
                .retain(new FieldList(allFieldNames));

        // add isPrimaryAccount = TRUE : if both domain/duns = not-null/empty
        String checkDomainDunsValue = String.format("%s != null && %s != null",
                config.getManualSeedDomain(), config.getManualSeedDuns());

        amSeedFields.add(config.getIsPrimaryAccount());
        Node addedPrimaryAccount = isPrimaryAccount //
                .apply(String.format("%s ? \"%s\" : \"%s\"", checkDomainDunsValue, PRIMARY_ACCOUNT_YES,
                        PRIMARY_ACCOUNT_NO),
                        new FieldList(config.getManualSeedDomain(), config.getManualSeedDuns()),
                        new FieldMetadata(config.getIsPrimaryAccount(), String.class)) //
                .retain(new FieldList(amSeedFields));

        return addedPrimaryAccount;
    }

}
