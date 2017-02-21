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
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedDomainRankBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedOrphanRecordSmallCompaniesBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterSeedOrphanRecordWithDomainBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.FillPrimaryDomainBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("accountMasterSeedMarkerTransformerFlow")
public class AccountMasterSeedMarkerRebuildFlow extends ConfigurableFlowBase<AccountMasterSeedMarkerConfig> {
    private static final Log log = LogFactory.getLog(AccountMasterSeedMarkerRebuildFlow.class);

    private static final String DUNS = "DUNS";
    private static final String DOMAIN = "Domain";
    private static final String LATTICE_ID = "LatticeID";
    private static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String ALEXA_RANK = "Rank";
    private static final String COUNTRY = "Country";
    private static final String LE_EMPLOYEE_RANGE = "LE_EMPLOYEE_RANGE";
    private static final String OUT_OF_BUSINESS_INDICATOR = "OUT_OF_BUSINESS_INDICATOR";
    private static final String URL_FIELD = "URL";

    private static final String FLAG_DROP_OOB_ENTRY = "_FLAG_DROP_OOB_ENTRY_";
    private static final String FLAG_DROP_SMALL_BUSINESS = "_FLAG_DROP_SMALL_BUSINESS_";
    private static final String FLAG_DROP_INCORRECT_DATA = "_FLAG_DROP_INCORRECT_DATA_";
    private static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";
    private static final String FLAG_DROP_ORPHAN_ENTRY = "_FLAG_DROP_ORPHAN_ENTRY_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node am = addSource(parameters.getBaseTables().get(0));
        Node alexaMostRecentNode = addSource(parameters.getBaseTables().get(1));

        // one of them do many things
        Node oobMkrd = markOOBEntries(am).renamePipe("oobMkrd");
        Node alexaMrkd = markLessPopularDomainsForDUNS(am, alexaMostRecentNode).renamePipe("alexaMrkd");
        Node orphanMrkd = markOrphanRecordWithDomain(am).renamePipe("orphanMrkd");
        Node badDataMrkd = markRecordsWithIncorrectIndustryRevenueEmployeeData(am).renamePipe("badDataMrkd");
        Node smBusiMrkd = markOrphanRecordsForSmallBusiness(am).renamePipe("smBusiMrkd");

        List<String> allFields = am.getFieldNames();
        allFields.add(FLAG_DROP_OOB_ENTRY);
        allFields.add(FLAG_DROP_SMALL_BUSINESS);
        allFields.add(FLAG_DROP_INCORRECT_DATA);
        allFields.add(FLAG_DROP_LESS_POPULAR_DOMAIN);
        allFields.add(FLAG_DROP_ORPHAN_ENTRY);
        FieldList finalFields = new FieldList(allFields.toArray(new String[allFields.size()]));

        am = am.discard(new FieldList(LE_IS_PRIMARY_DOMAIN));
        FieldList idField = new FieldList(LATTICE_ID);
        return am.coGroup(idField,
                Arrays.asList(badDataMrkd, oobMkrd, orphanMrkd, smBusiMrkd, alexaMrkd),
                Arrays.asList(idField, idField, idField, idField, idField),
                JoinType.INNER) //
                .retain(finalFields);
    }

    // (LID, FLAG_DROP_SMALL_BUSINESS)
    private Node markOrphanRecordsForSmallBusiness(Node node) {
        node = node.retain(new FieldList(LATTICE_ID, DUNS, LE_EMPLOYEE_RANGE)) //
                .addColumnWithFixedValue(FLAG_DROP_SMALL_BUSINESS, 1, Integer.class);

        // split by emp range
        Node orphanRecordWithDomainNode = node//
                .filter(DUNS + " != null &&" //
                        + LE_EMPLOYEE_RANGE + " != null && " //
                        + "(" + LE_EMPLOYEE_RANGE + ".equals(\"0\") || " //
                        + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") || "//
                        + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(DUNS, LE_EMPLOYEE_RANGE));
        Node remainingRecordNode = node//
                .filter(DUNS + " == null || " //
                        + LE_EMPLOYEE_RANGE + " == null || " //
                        + "(!" + LE_EMPLOYEE_RANGE + ".equals(\"0\") && " //
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") && "//
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(DUNS, LE_EMPLOYEE_RANGE));
        // apply buffer to one of them
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
        node = node.retain(
                new FieldList(LATTICE_ID, DUNS, DOMAIN, COUNTRY, LE_IS_PRIMARY_LOCATION, LE_NUMBER_OF_LOCATIONS));
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
        return notCheckOrphan.merge(checkOrphan);
    }

    // (LID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN)
    private Node markLessPopularDomainsForDUNS(Node node, Node alexaMostRecentNode) {
        node = node.retain(new FieldList(LATTICE_ID, DUNS, DOMAIN, LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_DOMAIN)) //
                .addColumnWithFixedValue(FLAG_DROP_LESS_POPULAR_DOMAIN, null, String.class);

        // split by domain and loc
        Node toJoinAlexa = node.filter(String.format("%s != null && %s != null && \"Y\".equalsIgnoreCase(%s)", DOMAIN,
                LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));
        Node notToJoinAlexa = node.filter(String.format("%s == null || %s == null || !\"Y\".equalsIgnoreCase(%s)",
                DOMAIN, LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));

        // one directly filter out fields
        notToJoinAlexa = notToJoinAlexa
                .retain(new FieldList(LATTICE_ID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN));

        // on of them join with alexa
        alexaMostRecentNode = alexaMostRecentNode.retain(new FieldList(URL_FIELD, ALEXA_RANK));
        toJoinAlexa = toJoinAlexa.join(new FieldList(DOMAIN), alexaMostRecentNode, new FieldList(URL_FIELD),
                JoinType.LEFT);

        // split by rank and domain
        Node hasAlexaRankAndLoc = toJoinAlexa//
                .filter(ALEXA_RANK + " != null && " + LE_IS_PRIMARY_DOMAIN + " != null && \"Y\"" + ".equalsIgnoreCase("
                        + LE_IS_PRIMARY_DOMAIN + ")", new FieldList(ALEXA_RANK, LE_IS_PRIMARY_DOMAIN));
        Node notHasAlexaAndLoc = toJoinAlexa//
                .filter(ALEXA_RANK + " == null || " + LE_IS_PRIMARY_DOMAIN + " == null || ! \"Y\""
                        + ".equalsIgnoreCase(" + LE_IS_PRIMARY_DOMAIN + ")",
                        new FieldList(ALEXA_RANK, LE_IS_PRIMARY_DOMAIN));

        // one of them apply DomainRankBuffer
        AccountMasterSeedDomainRankBuffer buffer = new AccountMasterSeedDomainRankBuffer(
                new Fields(DUNS, AccountMasterSeedDomainRankBuffer.MIN_RANK_DOMAIN));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DUNS, String.class));
        fms.add(new FieldMetadata(AccountMasterSeedDomainRankBuffer.MIN_RANK_DOMAIN, String.class));
        Node minRankDomain = hasAlexaRankAndLoc.groupByAndBuffer(new FieldList(DUNS), buffer, fms);
        minRankDomain = minRankDomain.renamePipe("minRankDomain");
        Node popularDomain = hasAlexaRankAndLoc.leftOuterJoin(new FieldList(DUNS), minRankDomain, new FieldList(DUNS));
        Fields joinNodeFields = new Fields(
                popularDomain.getFieldNames().toArray(new String[popularDomain.getFieldNames().size()]));
        popularDomain = popularDomain.groupByAndBuffer(new FieldList(DUNS), new FillPrimaryDomainBuffer(joinNodeFields)) //
                .retain(new FieldList(LATTICE_ID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN));

        // the other directly retain
        notHasAlexaAndLoc = notHasAlexaAndLoc
                .retain(new FieldList(LATTICE_ID, LE_IS_PRIMARY_DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN));

        // final merge
        return notHasAlexaAndLoc//
                .merge(popularDomain)//
                .merge(notToJoinAlexa);
    }

    // (LID, FLAG_DROP_OOB_ENTRY)
    private Node markOOBEntries(Node node) {
        node.retain(new FieldList(LATTICE_ID, OUT_OF_BUSINESS_INDICATOR));
        String markExpression = OUT_OF_BUSINESS_INDICATOR + " != null "//
                + "&& " + OUT_OF_BUSINESS_INDICATOR//
                + ".equals(\"1\") ";
        node = node.addFunction(markExpression + " ? 1 : 0 ", //
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
        return "accountMasterSeedMarkerTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterSeedMarkerTransformer";

    }
}