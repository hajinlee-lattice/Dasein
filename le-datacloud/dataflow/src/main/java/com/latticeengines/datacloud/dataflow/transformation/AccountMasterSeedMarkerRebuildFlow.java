package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
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
    private static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
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
        Node accountMasterIntermediateSeed = addSource(parameters.getBaseTables().get(0));

        Node alexaMostRecentNode = addSource(parameters.getBaseTables().get(1));

        accountMasterIntermediateSeed = addFlagColumnsWithDefaultValues(accountMasterIntermediateSeed);

        List<String> fieldNames = accountMasterIntermediateSeed.getFieldNames();
        Fields fieldDeclaration = new Fields(fieldNames.toArray(new String[fieldNames.size()]));
        for (String fieldName : fieldNames) {
            log.info("Field in input schema " + fieldName);
        }

        // split by duns
        Node nodeWithNullDUNS = accountMasterIntermediateSeed.filter(DUNS + " == null", new FieldList(DUNS));
        Node node = accountMasterIntermediateSeed.filter(DUNS + " != null", new FieldList(DUNS));

        // one of them do many things
        node = markOOBEntries(node);
        node = markLessPopularDomainsForDUNS(node, alexaMostRecentNode);
        node = markOrphanRecordWithDomain(node, fieldDeclaration);
        node = markRecordsWithIncorrectIndustryRevenueEmployeeData(node);
        node = markOrphanRecordsForSmallBusiness(node, fieldDeclaration);

        // merge back
        nodeWithNullDUNS.renamePipe("nullduns");
        node.renamePipe("mergeFinalResult");
        return nodeWithNullDUNS.merge(node).retain(new FieldList(fieldNames));
    }

    private Node addFlagColumnsWithDefaultValues(Node accountMasterIntermediateSeed) {
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_OOB_ENTRY, 0,
                Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_SMALL_BUSINESS,
                1, Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_INCORRECT_DATA,
                0, Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed
                .addColumnWithFixedValue(FLAG_DROP_LESS_POPULAR_DOMAIN, null, String.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_ORPHAN_ENTRY, 0,
                Integer.class);
        return accountMasterIntermediateSeed;
    }

    private Node markOrphanRecordsForSmallBusiness(Node node, Fields fieldDeclaration) {
        // split by emp range
        Node orphanRecordWithDomainNode = node//
                .filter(LE_EMPLOYEE_RANGE + " != null && " //
                        + "(" + LE_EMPLOYEE_RANGE + ".equals(\"0\") || " //
                        + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") || "//
                        + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(LE_EMPLOYEE_RANGE));
        Node remainingRecordNode = node//
                .filter(LE_EMPLOYEE_RANGE + " == null || " //
                        + "(!" + LE_EMPLOYEE_RANGE + ".equals(\"0\") && " //
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") && "//
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"11-50\"))", //
                        new FieldList(LE_EMPLOYEE_RANGE));

        // apply buffer to one of them
        AccountMasterSeedOrphanRecordSmallCompaniesBuffer buffer = new AccountMasterSeedOrphanRecordSmallCompaniesBuffer(
                fieldDeclaration);
        orphanRecordWithDomainNode = orphanRecordWithDomainNode.groupByAndBuffer(new FieldList(DUNS), buffer);

        // merge back
        orphanRecordWithDomainNode.renamePipe("checkorphansmallbusi");
        remainingRecordNode.renamePipe("notcheckorphansmallbusi");
        return remainingRecordNode.merge(orphanRecordWithDomainNode);
    }

    private Node markRecordsWithIncorrectIndustryRevenueEmployeeData(Node node) {
        return node;
    }

    private Node markOrphanRecordWithDomain(Node node, Fields fieldDeclaration) {
        // split by domain and loc
        Node checkOrphan = node.filter(String.format("%s != null && %s != null && \"Y\".equalsIgnoreCase(%s)", DOMAIN,
                LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));
        Node notCheckOrphan = node.filter(String.format("%s == null || %s == null || !\"Y\".equalsIgnoreCase(%s)",
                DOMAIN, LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));

        // apply buffer
        AccountMasterSeedOrphanRecordWithDomainBuffer buffer = new AccountMasterSeedOrphanRecordWithDomainBuffer(
                fieldDeclaration);
        checkOrphan = checkOrphan.groupByAndBuffer(new FieldList(COUNTRY, DOMAIN), buffer);

        // merge
        checkOrphan.renamePipe("checkorphan");
        notCheckOrphan.renamePipe("notcheckorphan");
        return notCheckOrphan.merge(checkOrphan);
    }

    private Node markLessPopularDomainsForDUNS(Node node, Node alexaMostRecentNode) {
        FieldList fieldsInNode = new FieldList(node.getFieldNames());

        // split by domain and loc
        Node toJoinAlexa = node.filter(String.format("%s != null && %s != null && \"Y\".equalsIgnoreCase(%s)", DOMAIN,
                LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));
        Node notToJoinAlexa = node.filter(String.format("%s == null || %s == null || !\"Y\".equalsIgnoreCase(%s)",
                DOMAIN, LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_LOCATION), new FieldList(DOMAIN, LE_IS_PRIMARY_LOCATION));

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
        minRankDomain.renamePipe("minRankDomain");
        Node popularDomain = hasAlexaRankAndLoc.leftOuterJoin(new FieldList(DUNS), minRankDomain, new FieldList(DUNS));
        Fields joinNodeFields = new Fields(
                popularDomain.getFieldNames().toArray(new String[popularDomain.getFieldNames().size()]));
        popularDomain = popularDomain.groupByAndBuffer(new FieldList(DUNS), new FillPrimaryDomainBuffer(joinNodeFields)) //
                .retain(new FieldList(hasAlexaRankAndLoc.getFieldNames()));

        // final merge
        notHasAlexaAndLoc = notHasAlexaAndLoc.retain(fieldsInNode);
        popularDomain = popularDomain.retain(fieldsInNode);
        notToJoinAlexa.renamePipe("notjoinalexa");
        popularDomain.renamePipe("popdomainbyalexa");
        notHasAlexaAndLoc.renamePipe("nothasalexarank");
        return notHasAlexaAndLoc//
                .merge(popularDomain)//
                .merge(notToJoinAlexa);
    }

    private Node markOOBEntries(Node node) {
        String markExpression = OUT_OF_BUSINESS_INDICATOR + " != null "//
                + "&& " + OUT_OF_BUSINESS_INDICATOR//
                + ".equals(\"1\") ";
        node = node.addFunction(markExpression + " ? 1 : 0 ", //
                new FieldList(OUT_OF_BUSINESS_INDICATOR), //
                new FieldMetadata(FLAG_DROP_OOB_ENTRY, Integer.class));
        return node;
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