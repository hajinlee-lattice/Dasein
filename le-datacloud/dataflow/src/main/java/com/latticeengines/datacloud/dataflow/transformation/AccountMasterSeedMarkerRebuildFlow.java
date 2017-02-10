package com.latticeengines.datacloud.dataflow.transformation;

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

        Node nodeWithNullDUNS = accountMasterIntermediateSeed//
                .filter(DUNS + " == null", new FieldList(DUNS));

        Node node = accountMasterIntermediateSeed//
                .filter(DUNS + " != null", new FieldList(DUNS));

        node = node.discard(new FieldList(FLAG_DROP_OOB_ENTRY));

        node = markOOBEntries(node, fieldDeclaration);

        node = node.retain(new FieldList(fieldNames));

        node = markLessPopularDomainsForDUNS(node, fieldDeclaration, alexaMostRecentNode);

        node = markOrphanRecordWithDomain(node, fieldDeclaration);

        node = markRecordsWithIncorrectIndustryRevenueEmployeeData(node);

        node = markOrphanRecordsForSmallBusiness(node, fieldDeclaration);

        node.renamePipe("mergeFinalResult");
        return nodeWithNullDUNS.merge(node).retain(new FieldList(fieldNames));
    }

    private Node addFlagColumnsWithDefaultValues(Node accountMasterIntermediateSeed) {
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_OOB_ENTRY, 0,
                Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_SMALL_BUSINESS,
                0, Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_INCORRECT_DATA,
                0, Integer.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed
                .addColumnWithFixedValue(FLAG_DROP_LESS_POPULAR_DOMAIN, null, String.class);
        accountMasterIntermediateSeed = accountMasterIntermediateSeed.addColumnWithFixedValue(FLAG_DROP_ORPHAN_ENTRY, 0,
                Integer.class);
        return accountMasterIntermediateSeed;
    }

    private Node markOrphanRecordsForSmallBusiness(Node node, Fields fieldDeclaration) {
        node.renamePipe("markOrphanRecordsForSmallBusiness");

        Node nonNullEmployeeRange = node//
                .filter(LE_EMPLOYEE_RANGE + " != null", new FieldList(LE_EMPLOYEE_RANGE));

        Node orphanRecordWithDomainNode = nonNullEmployeeRange//
                .filter(LE_EMPLOYEE_RANGE + ".equals(\"0\") || " //
                        + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") || "//
                        + LE_EMPLOYEE_RANGE + ".equals(\"11-50\")", //
                        new FieldList(LE_EMPLOYEE_RANGE));

        AccountMasterSeedOrphanRecordSmallCompaniesBuffer buffer = new AccountMasterSeedOrphanRecordSmallCompaniesBuffer(
                fieldDeclaration);
        orphanRecordWithDomainNode = orphanRecordWithDomainNode//
                .groupByAndBuffer(new FieldList(DUNS), buffer);

        Node remainingRecordNode = node//
                .filter(LE_EMPLOYEE_RANGE + " == null", new FieldList(LE_EMPLOYEE_RANGE));
        remainingRecordNode = remainingRecordNode.merge(nonNullEmployeeRange//
                .filter("!" + LE_EMPLOYEE_RANGE + ".equals(\"0\") && " //
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"1-10\") && "//
                        + "!" + LE_EMPLOYEE_RANGE + ".equals(\"11-50\")", //
                        new FieldList(LE_EMPLOYEE_RANGE)));

        node = remainingRecordNode.merge(orphanRecordWithDomainNode);
        return node;
    }

    private Node markRecordsWithIncorrectIndustryRevenueEmployeeData(Node node) {
        node.renamePipe("markRecordsWithIncorrectIndustryRevenueEmployeeData");

        return node;
    }

    private Node markOrphanRecordWithDomain(Node node, Fields fieldDeclaration) {
        node.renamePipe("markOrphanRecordWithDomain");

        Node nonNullDomainNode = node//
                .filter(DOMAIN + " != null", new FieldList(DOMAIN));
        Node nonNullLocationNode = nonNullDomainNode//
                .filter(LE_IS_PRIMARY_LOCATION + " != null", new FieldList(LE_IS_PRIMARY_LOCATION));
        Node orphanRecordWithDomainNode = nonNullLocationNode//
                .filter("\"Y\"" + ".equalsIgnoreCase(" + LE_IS_PRIMARY_LOCATION + ")",
                        new FieldList(LE_IS_PRIMARY_LOCATION));
        AccountMasterSeedOrphanRecordWithDomainBuffer buffer = new AccountMasterSeedOrphanRecordWithDomainBuffer(
                fieldDeclaration);
        orphanRecordWithDomainNode = orphanRecordWithDomainNode//
                .groupByAndBuffer(new FieldList(COUNTRY, DOMAIN), buffer);

        Node remainingRecordNode = node//
                .filter(DOMAIN + " == null", new FieldList(DOMAIN));
        remainingRecordNode = remainingRecordNode.merge(nonNullDomainNode//
                .filter(LE_IS_PRIMARY_LOCATION + " == null", new FieldList(LE_IS_PRIMARY_LOCATION)));
        remainingRecordNode = remainingRecordNode.merge(nonNullLocationNode//
                .filter("! \"Y\"" + ".equalsIgnoreCase(" + LE_IS_PRIMARY_LOCATION + ")",
                        new FieldList(LE_IS_PRIMARY_LOCATION)));

        node = remainingRecordNode.merge(orphanRecordWithDomainNode);
        return node;
    }

    private Node markLessPopularDomainsForDUNS(Node node, Fields fieldDeclaration, Node alexaMostRecentNode) {
        node.renamePipe("markLessPopularDomainsForDUNS");

        FieldList fieldsInNode = new FieldList(node.getFieldNames());
        Node nonNullLocationNode = node//
                .filter(DOMAIN + " != null && " + LE_IS_PRIMARY_DOMAIN + " != null",
                        new FieldList(DOMAIN, LE_IS_PRIMARY_DOMAIN));
        Node nodeForJoiningWithAlexaMostRecent = nonNullLocationNode//
                .filter("\"Y\"" + ".equalsIgnoreCase(" + LE_IS_PRIMARY_DOMAIN + ")",
                        new FieldList(LE_IS_PRIMARY_DOMAIN));

        Node nodeNotForJoiningWithAlexaMostRecent = node//
                .filter(DOMAIN + " == null || " + LE_IS_PRIMARY_DOMAIN + " == null",
                        new FieldList(DOMAIN, LE_IS_PRIMARY_DOMAIN));
        nodeNotForJoiningWithAlexaMostRecent = //
                nodeNotForJoiningWithAlexaMostRecent.merge(nonNullLocationNode.filter(
                        "! \"Y\"" + //
                                ".equalsIgnoreCase(" + LE_IS_PRIMARY_DOMAIN + ")", //
                        new FieldList(LE_IS_PRIMARY_DOMAIN)));

        alexaMostRecentNode = alexaMostRecentNode.retain(new FieldList(URL_FIELD, ALEXA_RANK));

        nodeForJoiningWithAlexaMostRecent = nodeForJoiningWithAlexaMostRecent.join(new FieldList(DOMAIN),
                alexaMostRecentNode, new FieldList(URL_FIELD), JoinType.LEFT);

        Node nonNullAlexaRankAndLocationnodeForJoiningWithAlexaMostRecent = //
                nodeForJoiningWithAlexaMostRecent//
                        .filter(ALEXA_RANK + " != null && " + LE_IS_PRIMARY_DOMAIN + " != null ",
                                new FieldList(ALEXA_RANK, LE_IS_PRIMARY_DOMAIN));
        Node accountMasterSeedPopularDomainRecordNode = nonNullAlexaRankAndLocationnodeForJoiningWithAlexaMostRecent//
                .filter("\"Y\"" + ".equalsIgnoreCase(" + LE_IS_PRIMARY_DOMAIN + ")",
                        new FieldList(LE_IS_PRIMARY_DOMAIN));

        Fields fieldDeclarationExpanded = new Fields(accountMasterSeedPopularDomainRecordNode.getFieldNames()
                .toArray(new String[accountMasterSeedPopularDomainRecordNode.getFieldNames().size()]));
        AccountMasterSeedDomainRankBuffer buffer = new AccountMasterSeedDomainRankBuffer(fieldDeclarationExpanded);
        accountMasterSeedPopularDomainRecordNode = accountMasterSeedPopularDomainRecordNode//
                .groupByAndBuffer(new FieldList(DUNS), buffer);

        Node remainingRecordNode = nodeForJoiningWithAlexaMostRecent//
                .filter(ALEXA_RANK + " == null || " + LE_IS_PRIMARY_DOMAIN + " == null ",
                        new FieldList(ALEXA_RANK, LE_IS_PRIMARY_DOMAIN));
        remainingRecordNode = remainingRecordNode.merge(//
                nonNullAlexaRankAndLocationnodeForJoiningWithAlexaMostRecent//
                        .filter("! \"Y\"" + ".equalsIgnoreCase(" + LE_IS_PRIMARY_DOMAIN + ")",
                                new FieldList(LE_IS_PRIMARY_DOMAIN)));

        remainingRecordNode = remainingRecordNode.retain(fieldsInNode);
        accountMasterSeedPopularDomainRecordNode = accountMasterSeedPopularDomainRecordNode.retain(fieldsInNode);

        return remainingRecordNode//
                .merge(accountMasterSeedPopularDomainRecordNode)//
                .merge(nodeNotForJoiningWithAlexaMostRecent);
    }

    private Node markOOBEntries(Node node, Fields fieldDeclaration) {
        node.renamePipe("markOOBEntries");

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