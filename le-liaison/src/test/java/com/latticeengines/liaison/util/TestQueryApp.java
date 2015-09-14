package com.latticeengines.liaison.util;


import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.service.impl.QueryVDBImpl;
import com.latticeengines.liaison.exposed.service.QueryColumn;

public class TestQueryApp {

	public static void main(String[] args) {
		
		String spec = "SpecLatticeQuery(LatticeAddressSetPushforward(LatticeAddressExpressionFromLAS(LatticeAddressSetMeet((LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Sys_LatticeSystemID\"))))))), LatticeAddressSetMeet((LatticeAddressSetPi(LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"Sys_LatticeSystemID\")))))), LatticeAddressExpressionMeet((LatticeAddressExpressionAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))))), SpecQueryNamedFunctions(SpecQueryNamedFunctionExpression(ContainerElementName(\"ELQ_Contact_ContactID\"), LatticeFunctionIdentifierAddressAtomic(LatticeAddressAtomicIdentifier(ContainerElementName(\"ELQ_Contact_ContactID\")))) SpecQueryNamedFunctionEntityFunctionBoundary SpecQueryNamedFunctionExpression(ContainerElementName(\"Time_OfSubmission_PushToScoring\"), LatticeFunctionExpressionConstant(\"Now\", DataTypeDateTime))), SpecQueryResultSetAll)";
		Query q = new QueryVDBImpl( "Q_Timestamp_PreScoringIncr", spec );
		
		System.out.println( String.format("Query name: %s",q.getName()) );
		System.out.println( "Filter definitions:" );
		int i = 1;
		for( String f : q.getFilterDefns() ) {
			System.out.println( String.format("  (%d) %s",i,f) );
			i++;
		}
		System.out.println( "Entity definitions:" );
		i = 1;
		for( String e : q.getEntityDefns() ) {
			System.out.println( String.format("  (%d) %s",i,e) );
			i++;
		}
		System.out.println( "Columns:" );
		i = 1;
		for( QueryColumn qc : q.getColumns() ) {
			System.out.println( String.format("  (%d) %s",i,qc.getName()) );
			i++;
		}
		
	}

}
