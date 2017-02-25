package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedOrphanRecordSmallCompaniesBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedOrphanRecordWithDomainBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedPrimaryDomainAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(AMSeedMarker.DATAFLOW_BEAN_NAME)
public class AMSeedMarker extends AccountMasterBase<AccountMasterSeedMarkerConfig> {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AMSeedMarker.class);

    public static final String DATAFLOW_BEAN_NAME = "AMSeedMarker";
    public static final String TRANSFORMER_NAME = "AMSeedMarkerTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node am = addSource(parameters.getBaseTables().get(0));
        Node alexa = addSource(parameters.getBaseTables().get(1));

        // Add AlexaRank to AMSeed (AlexaRank is needed in other dataflow)
        am = addAlexaRank(am, alexa);

        // one of them do many things
        Node oobMkrd = markOOBEntries(am).renamePipe("oobMkrd");
        Node orphanMrkd = markOrphanRecordWithDomain(am).renamePipe("orphanMrkd");
        Node badDataMrkd = markRecordsWithIncorrectIndustryRevenueEmployeeData(am).renamePipe("badDataMrkd");
        Node smBusiMrkd = markOrphanRecordsForSmallBusiness(am).renamePipe("smBusiMrkd");
        Node alexaMrkd = markLessPopularDomainsForDUNS(am).renamePipe("alexaMrkd");

        List<String> allFields = am.getFieldNames();
        allFields.add(FLAG_DROP_OOB_ENTRY);
        allFields.add(FLAG_DROP_SMALL_BUSINESS);
        allFields.add(FLAG_DROP_INCORRECT_DATA);
        allFields.add(FLAG_DROP_LESS_POPULAR_DOMAIN);
        allFields.add(FLAG_DROP_ORPHAN_ENTRY);
        FieldList finalFields = new FieldList(allFields.toArray(new String[allFields.size()]));

        FieldList idField = new FieldList(LATTICE_ID);
        am = am.discard(new FieldList(LE_IS_PRIMARY_DOMAIN));

        /*
        if ("tez".equalsIgnoreCase(parameters.getEngineConfiguration().getEngine())) {
            badDataMrkd = badDataMrkd.checkpoint();
            oobMkrd = oobMkrd.checkpoint();
            smBusiMrkd = smBusiMrkd.checkpoint();
            orphanMrkd = orphanMrkd.checkpoint();
            am = am.checkpoint();
            alexaMrkd = alexaMrkd.checkpoint();
        }
        */

        return am.coGroup(idField, //
                Arrays.asList(badDataMrkd, oobMkrd, orphanMrkd, smBusiMrkd, alexaMrkd), //
                        Arrays.asList(idField, idField, idField, idField, idField), //
                JoinType.INNER).retain(finalFields);
        /*        
        return am.join(idField, oobMkrd, idField, JoinType.OUTER) //
                .join(idField, badDataMrkd, idField, JoinType.OUTER) //
                .join(idField, orphanMrkd, idField, JoinType.OUTER) //
                .join(idField, smBusiMrkd, idField, JoinType.OUTER) //
                .join(idField, alexaMrkd, idField, JoinType.OUTER) //
                .retain(finalFields);
         */

    }

    private Node addAlexaRank(Node ams, Node alexa) {
        alexa = alexa.retain(new FieldList(ALEXA_URL, ALEXA_RANK));
        Node amsDomain = ams.filter(String.format("%s != null", DOMAIN), new FieldList(DOMAIN));
        Node amsNoDomain = ams.filter(String.format("%s == null", DOMAIN), new FieldList(DOMAIN));
        amsNoDomain = amsNoDomain.addColumnWithFixedValue(ALEXA_RANK_AMSEED, null, Integer.class);

        amsDomain = amsDomain.join(new FieldList(DOMAIN), alexa, new FieldList(ALEXA_URL), JoinType.LEFT);
        amsDomain = amsDomain.discard(new FieldList(ALEXA_URL)).rename(new FieldList(ALEXA_RANK),
                new FieldList(ALEXA_RANK_AMSEED));
        return amsDomain.merge(amsNoDomain);
    }

    // (LID, FLAG_DROP_SMALL_BUSINESS)
    private Node markOrphanRecordsForSmallBusiness(Node node) {
        // split by emp range
        Node orphanRecordWithDomainNode = node//
                .filter(DUNS + " != null &&" //
                        + LE_EMPLOYEE_RANGE + " != null && " //
                        + "(" + LE_EMPLOYEE_RANGE + ".equals(\"0\") || " //
                        + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") || "//
                        + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(DUNS, LE_EMPLOYEE_RANGE))
                .addColumnWithFixedValue(FLAG_DROP_SMALL_BUSINESS, 1, Integer.class);
        Node remainingRecordNode = node//
                .filter(DUNS + " == null || " //
                        + LE_EMPLOYEE_RANGE + " == null || " //
                        + "(!" + LE_EMPLOYEE_RANGE + ".equals(\"0\") && " //
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") && "//
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(DUNS, LE_EMPLOYEE_RANGE))
                .addColumnWithFixedValue(FLAG_DROP_SMALL_BUSINESS, 0, Integer.class);
        // apply buffer to one of them. this buffer needs all the attributes in ams. do not retain fields in the node beforehand
        AccountMasterSeedOrphanRecordSmallCompaniesBuffer buffer = new AccountMasterSeedOrphanRecordSmallCompaniesBuffer(
                new Fields(orphanRecordWithDomainNode.getFieldNamesArray()));
        orphanRecordWithDomainNode = orphanRecordWithDomainNode.groupByAndBuffer(new FieldList(DUNS), buffer);

        // merge back
        Node result = remainingRecordNode.merge(orphanRecordWithDomainNode);
        return result.retain(new FieldList(LATTICE_ID, FLAG_DROP_SMALL_BUSINESS));
    }

    // (LID, FLAG_DROP_INCORRECT_DATA)
    private Node markRecordsWithIncorrectIndustryRevenueEmployeeData(Node node) {
        return node.retain(new FieldList(LATTICE_ID)) //
                .addColumnWithFixedValue(FLAG_DROP_INCORRECT_DATA, 0, Integer.class);
    }

    // (LID, FLAG_DROP_ORPHAN_ENTRY)
    private Node markOrphanRecordWithDomain(Node node) {
        node = node.retain(new FieldList(LATTICE_ID, DUNS, DOMAIN, COUNTRY, LE_IS_PRIMARY_LOCATION,
                LE_NUMBER_OF_LOCATIONS, SALES_VOLUME_US_DOLLARS));
        node = node.addColumnWithFixedValue(FLAG_DROP_ORPHAN_ENTRY, 0, Integer.class);

        // split by domain and loc
        Node checkOrphan = node.filter(
                String.format("%s != null && %s != null && %s != null && \"Y\".equalsIgnoreCase(%s)", DUNS, DOMAIN,
                        LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION),
                new FieldList(DUNS, DOMAIN, LE_IS_PRIMARY_LOCATION));
        Node notCheckOrphan = node.filter(
                String.format("%s == null || %s == null || %s == null || !\"Y\".equalsIgnoreCase(%s)", DUNS, DOMAIN,
                        LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION),
                new FieldList(DUNS, DOMAIN, LE_IS_PRIMARY_LOCATION));

        // one directly retain
        notCheckOrphan = notCheckOrphan.retain(new FieldList(LATTICE_ID, FLAG_DROP_ORPHAN_ENTRY));

        // apply buffer
        AccountMasterSeedOrphanRecordWithDomainBuffer buffer = new AccountMasterSeedOrphanRecordWithDomainBuffer(
                new Fields(checkOrphan.getFieldNamesArray()));
        checkOrphan = checkOrphan.groupByAndBuffer(new FieldList(COUNTRY, DOMAIN), buffer) //
                .retain(new FieldList(LATTICE_ID, FLAG_DROP_ORPHAN_ENTRY));

        // merge
        return notCheckOrphan.merge(checkOrphan).retain(new FieldList(LATTICE_ID, FLAG_DROP_ORPHAN_ENTRY));
    }

    // (LID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN)
    @SuppressWarnings("rawtypes")
    private Node markLessPopularDomainsForDUNS(Node node) {
        node = node.retain(
                new FieldList(LATTICE_ID, DUNS, DOMAIN, LE_IS_PRIMARY_DOMAIN, ALEXA_RANK_AMSEED, DOMAIN_SOURCE));

        Node withDuns = node.filter(String.format("%s != null", DUNS), new FieldList(DUNS));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DUNS, String.class));
        fms.add(new FieldMetadata(FLAG_DROP_LESS_POPULAR_DOMAIN, String.class));
        Aggregator agg = new AccountMasterSeedPrimaryDomainAggregator(new Fields(DUNS, FLAG_DROP_LESS_POPULAR_DOMAIN),
                DUNS, FLAG_DROP_LESS_POPULAR_DOMAIN, DOMAIN, ALEXA_RANK_AMSEED, DOMAIN_SOURCE, LE_IS_PRIMARY_DOMAIN);
        Node primaryDomain = withDuns.groupByAndAggregate(new FieldList(DUNS), agg, fms).renamePipe("PrimaryDomain");

        node = node.join(new FieldList(DUNS), primaryDomain, new FieldList(DUNS), JoinType.LEFT);
        node = node.discard(new FieldList(LE_IS_PRIMARY_DOMAIN));
        // No domain || domain != primary domain: IsPrimaryDomain = N
        // Has domain, no primary domain || domain = primary domain: IsPrimaryDomain = Y
        node = node.apply(
                String.format("%s == null ? \"N\" : ((%s == null || %s.equals(%s)) ? \"Y\" : \"N\")", DOMAIN,
                        FLAG_DROP_LESS_POPULAR_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN, DOMAIN), //
                new FieldList(DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN), //
                new FieldMetadata(LE_IS_PRIMARY_DOMAIN, String.class));
        node = node.retain(new FieldList(LATTICE_ID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN));
        return node;
    }

    // (LID, FLAG_DROP_OOB_ENTRY)
    private Node markOOBEntries(Node node) {
        node.retain(new FieldList(LATTICE_ID, OUT_OF_BUSINESS_INDICATOR));
        String markExpression = OUT_OF_BUSINESS_INDICATOR + " != null "//
                + "&& " + OUT_OF_BUSINESS_INDICATOR//
                + ".equals(\"1\") ";
        node = node.apply(markExpression + " ? 1 : 0 ", //
                new FieldList(OUT_OF_BUSINESS_INDICATOR), //
                new FieldMetadata(FLAG_DROP_OOB_ENTRY, Integer.class));
        return node.retain(new FieldList(LATTICE_ID, FLAG_DROP_OOB_ENTRY));
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AccountMasterSeedMarkerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
