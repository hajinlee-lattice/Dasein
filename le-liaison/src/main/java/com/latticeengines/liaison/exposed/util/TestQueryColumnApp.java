package com.latticeengines.liaison.exposed.util;

import java.util.Map;

import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.liaison.service.impl.QueryColumnVDBImpl;

public class TestQueryColumnApp {

	public static void main(String[] args) {
		
		String spec = "SpecQueryNamedFunctionMetadata(SpecQueryNamedFunctionExpression(ContainerElementName(\"Industry_Group\"), LatticeFunctionIdentifier(ContainerElementName(\"DS_Industry_Group\"))), SpecExtractDetails((SpecExtractDetail(\"DisplayName\", \"Industry Rollup\"), SpecExtractDetail(\"Description\", \"Rollup of Industry field from Marketing Automation, \\\"Good Times\\\"\"), SpecExtractDetail(\"ApprovedUsage\", StringList(\"ModelAndAllInsights\")), SpecExtractDetail(\"StatisticalType\", \"nominal\"), SpecExtractDetail(\"Tags\", StringList(\"Internal\")), SpecExtractDetail(\"Category\", \"Lead Information\"))))";
		QueryColumn qc = new QueryColumnVDBImpl( spec );
		
		System.out.println( String.format("Metadata for column \"%s\":",qc.getName()) );
		
		Map<String,String> metadata = qc.getMetadata();
		for( Map.Entry<String,String> entry : metadata.entrySet() ) {
			System.out.println( String.format("  * %23s: %s",entry.getKey(),entry.getValue()) );
		}
		
		System.out.println( "Generated definition:" );
		System.out.println( qc.definition() );

	}

}
