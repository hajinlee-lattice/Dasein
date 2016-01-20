package com.latticeengines.liaison.service.impl;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.liaison.testframework.LiaisonTestNGBase;

public class QueryVDBImplUnitTestNG extends LiaisonTestNGBase {

    private static final String queryDefn01 = "SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionFromLAS(LatticeAddressSetMeet((LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName(\"ELQ_Contact_ContactID_IsSelectedForPushToDestination\")), LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName(\"Timestamp_PushToDante\")), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Timestamp_PushToDante\")))))))), LatticeAddressSetMeet((LatticeAddressSetFcn(LatticeFunctionIdentifier(ContainerElementName(\"ELQ_Contact_ContactID_IsSelectedForPushToDestination\")), LatticeAddressSetSourceTable(LatticeSourceTableIdentifier(ContainerElementName(\"Timestamp_PushToDante\")), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Timestamp_PushToDante\"))))))), LatticeAddressExpressionMeet((LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName(\"ContactID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))) SpecQueryNamedFunctionEntityFunctionBoundary SpecQueryNamedFunctionExpression(ContainerElementName(\"C_Lattice_LastScoreDate1\"), LatticeFunctionExpressionConstant(\"Now\", DataTypeDateTime)) SpecQueryNamedFunctionExpression(ContainerElementName(\"C_Lattice_Predictive_Score1\"), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")), ContainerElementName(\"Percentile\"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")))), FunctionAggregationSelectWhere(FunctionAggregationOperator(\"Max\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")), ContainerElementName(\"ScoreDate\"))), FunctionAggregationOperator(\"Max\"))))), SpecQueryResultSetAll)";
    private static final String queryDefn07 = "SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Bard_LeadScoreStage\"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Bard_LeadScoreStage\")))), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName(\"ContactID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))) SpecQueryNamedFunctionEntityFunctionBoundary SpecQueryNamedFunctionExpression(ContainerElementName(\"C_Lattice_LastScoreDate1\"), LatticeFunctionExpressionConstant(\"Now\", DataTypeDateTime)) SpecQueryNamedFunctionExpression(ContainerElementName(\"C_Lattice_Predictive_Score1\"), LatticeFunctionExpressionTransform(LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")), ContainerElementName(\"Percentile\"))), LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")))), FunctionAggregationSelectWhere(FunctionAggregationOperator(\"Max\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"Bard_LeadScoreHistory\")), ContainerElementName(\"ScoreDate\"))), FunctionAggregationOperator(\"Max\"))))), SpecQueryResultSetAll)";
    private static final String queryDefn08 = "SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Bard_LeadScore\"))), LatticeAddressSetIdentifier(ContainerElementName(\"SelectedForDante\")), LatticeAddressExpressionMeet((LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"SFDC_Contact_ID\"))), LatticeAddressExpressionAtomic(LatticeAddressAtomicFTOA(LatticeFunctionIdentifier(ContainerElementName(\"LeadID_50Char\")), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementName(\"LeadID_50Char\")))))))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName(\"SFDC_Contact_ID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"SFDC_Contact_ID\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"LeadID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicFTOA(LatticeFunctionIdentifier(ContainerElementName(\"LeadID_50Char\")), LatticeAddressSetFromFcnSupport(LatticeFunctionIdentifier(ContainerElementName(\"LeadID_50Char\")))))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionEntityFunctionBoundary SpecQueryNamedFunctionExpression(ContainerElementName(\"AttributeName\"), LatticeFunctionIdentifierFunctionName) SpecQueryNamedFunctionExpression(ContainerElementName(\"AttributeValue\"), LatticeFunctionIdentifierFunctionValue) SpecQueryNamedFunctionExpression(ContainerElementName(\"WidgetsTopAttributes\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"PD_DerivedColumns\")), ContainerElementName(\"WidgetsTopAttributes\"))))), SpecQueryResultSetAll)";
    private static final String queryDefn09 = "SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"SFDC_Lead_Contact_ID\"))), LatticeAddressSetIdentifier(ContainerElementName(\"SelectedForDante\")), LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"SFDC_Lead_Contact_ID\")))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName(\"SFDC_Lead_Contact_ID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"SFDC_Lead_Contact_ID\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"ModelingID\"), LatticeFunctionExpressionTransform(LatticeFunctionExpressionRowIndex(LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicAll)), LatticeFunctionExpressionOrderedList(LatticeFunctionExpressionOrdered(LatticeFunctionIdentifier(ContainerElementName(\"SFDC_Id\")), SpecSortDirectionAscending))), LatticeAddressSetIdentifier(ContainerElementName(\"SelectedForModeling_SFDC\")), FunctionAggregationOperator(\"None\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"Email\"), LatticeFunctionIdentifier(ContainerElementName(\"SFDC_Email\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"CreationDate\"), LatticeFunctionIdentifier(ContainerElementName(\"SFDC_CreatedDate\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"Company\"), LatticeFunctionIdentifier(ContainerElementName(\"SFDC_Company\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"LastName\"), LatticeFunctionIdentifier(ContainerElementName(\"SFDC_LastName\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"FirstName\"), LatticeFunctionIdentifier(ContainerElementName(\"SFDC_FirstName\"))), SpecExtractDetails((SpecExtractDetail(\"ApprovedUsage\", \"None\")))) SpecQueryNamedFunctionExpression(ContainerElementName(\"AttributeName\"), LatticeFunctionIdentifierFunctionName) SpecQueryNamedFunctionExpression(ContainerElementName(\"AttributeValue\"), LatticeFunctionIdentifierFunctionValue) SpecQueryNamedFunctionExpression(ContainerElementName(\"WidgetsTopAttributes\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"PD_DerivedColumns\")), ContainerElementName(\"WidgetsTopAttributes\")))) SpecQueryNamedFunctionExpression(ContainerElementName(\"WebsiteFrameworksTopAttributes\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"PD_DerivedColumns\")), ContainerElementName(\"WebsiteFrameworksTopAttributes\")))) SpecQueryNamedFunctionExpression(ContainerElementName(\"WebsiteEncodingStandardsTopAttributes\"), LatticeFunctionIdentifier(ContainerElementNameTableQualifiedName(LatticeSourceTableIdentifier(ContainerElementName(\"PD_DerivedColumns\")), ContainerElementName(\"WebsiteEncodingStandardsTopAttributes\"))))), SpecQueryResultSetAll)";

    @Test(groups = "unit")
    public void testQueryParsing() {

        QueryVDBImpl q01 = new QueryVDBImpl("queryDefn01", queryDefn01);
        assertEquals(q01.getLASPatternMatched(), "pattern_las_meet");

        QueryVDBImpl q07 = new QueryVDBImpl("queryDefn07", queryDefn07);
        assertEquals(q07.getLASPatternMatched(), "pattern_las_expatomic_setPI_atomic");

        QueryVDBImpl q08 = new QueryVDBImpl("queryDefn08", queryDefn08);
        assertEquals(q08.getLASPatternMatched(), "pattern_las_expatomic_setID_meet");

        QueryVDBImpl q09 = new QueryVDBImpl("queryDefn09", queryDefn09);
        assertEquals(q09.getLASPatternMatched(), "pattern_las_expatomic_setID_atomic");
    }

}
