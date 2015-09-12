package com.latticeengines.liaison.service.impl;

import com.latticeengines.liaison.exposed.exception.DefinitionException;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.liaison.service.impl.QueryColumnVDBImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class QueryVDBImpl extends Query {
	
	private static final Set<String> lasNotImpl = new HashSet<String>(
		Arrays.asList(
			"Fcn","AndNot","Meet","Join","Pushforward","Pullback","Relation","Copy"
			,"Port","Level","Conflict","PivotFunctions"
		)
	);
	
	private static final Set<String> laeNotImpl = new HashSet<String>(
		Arrays.asList(
			"Join","Meet","Copy","Relation","DivideIntoExistingNode","DivideNewNode","Port"
		)
	);
	
	private static final Pattern pattern_slq = Pattern.compile("^SpecLatticeQuery\\((LatticeAddressSet.*), SpecQueryNamedFunctions\\((.*)\\), (SpecQueryResultSet.*)\\)$");
	private static final Pattern pattern_las_atomic = Pattern.compile("^LatticeAddressSetPushforward\\((LatticeAddressExpression.*), LatticeAddressSetMeet\\((.*?)\\), (LatticeAddressExpressionAtomic\\(.*?\\))\\)$");
	private static final Pattern pattern_las_meet = Pattern.compile("^LatticeAddressSetPushforward\\((LatticeAddressExpression.*), LatticeAddressSetMeet\\((.*?)\\), LatticeAddressExpressionMeet\\((.*?)\\)\\)$");
	private static final Pattern pattern_filter_fcn = Pattern.compile("^(LatticeAddressSetFcn\\(LatticeFunction.*?LatticeAddressSet.*?\\))(, LatticeAddressSet.*|$)");
	private static final Pattern pattern_filter_las = Pattern.compile("^(LatticeAddressSet(.*?)\\((.*?)\\))(, LatticeAddressSet.*|$)");
	private static final Pattern pattern_entity_lae = Pattern.compile("^(LatticeAddressExpression(.*?)\\((.*?)\\))(, LatticeAddressExpression.*|$)");
	private static final Pattern pattern_sqnf = Pattern.compile("^(SpecQueryNamedFunction.*?)( SpecQueryNamedFunction.*|$)");

	public QueryVDBImpl( String name, String definition ) throws DefinitionException {
		super( name, definition );
	}
	
	public QueryVDBImpl( QueryVDBImpl other ) {
		super( other );
	}
	
	public QueryColumn createColumn( QueryColumn qc ) {
		QueryColumnVDBImpl vdbcol = (QueryColumnVDBImpl)qc;
		return new QueryColumnVDBImpl( vdbcol );
	}
	
	public void initFromDefinition( String name, String defn ) throws DefinitionException {
		
		List<QueryColumn> columns = new ArrayList<>();
		List<String> filterDefns = new ArrayList<>();
		List<String> entityDefns = new ArrayList<>();
		String resultSetDefn = "NOT SET";
		
		String las_spec;
		String sqnf;
		String lasm = "NOT SET";
		String lae = "NOT SET";
		boolean isMatchedTopLevel = Boolean.FALSE;
		
		Matcher m_slq = pattern_slq.matcher( defn );
		if( !m_slq.matches() ) {
			throw new DefinitionException( String.format("Maude definition (SpecLatticeQuery) cannot be interpretted: %s",defn) );
		}
		
		las_spec = m_slq.group(1);
		sqnf = m_slq.group(2);
		resultSetDefn = m_slq.group(3);
		
		if( !isMatchedTopLevel ) {
			Matcher m_las_atomic = pattern_las_atomic.matcher( las_spec );
			if( m_las_atomic.matches() ) {
				lasm = m_las_atomic.group(2);
				lae = "(" + m_las_atomic.group(3) + ")";
				isMatchedTopLevel = Boolean.TRUE;
			}
		}
		
		if( !isMatchedTopLevel ) {
			Matcher m_las_meet = pattern_las_meet.matcher( las_spec );
			if( m_las_meet.matches() ) {
				lasm = m_las_meet.group(2);
				lae = m_las_meet.group(3);
				isMatchedTopLevel = Boolean.TRUE;
			}
		}
		
		if( !isMatchedTopLevel ) {
			throw new DefinitionException( String.format("Maude definition (LatticeAddressSet) cannot be interpretted: %s",las_spec) );
		}
		
		if( !lasm.equals("empty") ) {
			lasm = lasm.substring(1,lasm.length()-1);
			while(Boolean.TRUE) {
				
				String filterspec = "";
				String remainingspec = "";
				boolean isMatched = Boolean.FALSE;
				
				if( !isMatched ) {
					Matcher m_filter_fcn = pattern_filter_fcn.matcher( lasm );
					if( m_filter_fcn.matches() ) {
						filterspec = m_filter_fcn.group(1);
						remainingspec = m_filter_fcn.group(2);
						isMatched = Boolean.TRUE;
					}
				}
				
				if( !isMatched ) {
					Matcher m_filter_las = pattern_filter_las.matcher( lasm );
					if( m_filter_las.matches() ) {
						if( lasNotImpl.contains(m_filter_las.group(2)) ) {
							throw new DefinitionException( String.format("Maude definition LatticeAddressSet%s not implemented",m_filter_las.group(2)) );
						}
						filterspec = m_filter_las.group(1);
						remainingspec = m_filter_las.group(4);
						isMatched = Boolean.TRUE;
					}
				}
				
				if( !isMatched ) {
					throw new DefinitionException( String.format("Maude definition (LatticeAddressSet) cannot be interpretted: %s",lasm) );
				}
				
				filterDefns.add( filterspec );
				if( remainingspec.equals("") ) {
					break;
				}
				else {
					lasm = remainingspec.substring(2);
				}
			}
		}
		
		if( !lae.equals("empty") ) {
			lae = lae.substring(1,lae.length()-1);
			while(Boolean.TRUE) {
				
				String entityspec = "";
				String remainingspec = "";
				boolean isMatched = Boolean.FALSE;
				
				if( !isMatched ) {
					Matcher m_entity_lae = pattern_entity_lae.matcher( lae );
					if( m_entity_lae.matches() ) {
						if( laeNotImpl.contains(m_entity_lae.group(2)) ) {
							throw new DefinitionException( String.format("Maude definition LatticeAddressExpression%s not implemented",m_entity_lae.group(2)) );
						}
						entityspec = m_entity_lae.group(1);
						remainingspec = m_entity_lae.group(4);
						isMatched = Boolean.TRUE;
					}
				}
				
				if( !isMatched ) {
					throw new DefinitionException( String.format("Maude definition (LatticeAddressExpression) cannot be interpretted: %s",lae) );
				}
				
				entityDefns.add( entityspec );
				if( remainingspec.equals("") ) {
					break;
				}
				else {
					lae = remainingspec.substring(2);
				}
			}
		}
		
		while(Boolean.TRUE) {
			Matcher m_sqnf = pattern_sqnf.matcher( sqnf );
			if( !m_sqnf.matches() ) {
				throw new DefinitionException( String.format("Maude definition (SpecQueryNamedFunction) cannot be interpretted: %s",sqnf) );
			}
			columns.add( new QueryColumnVDBImpl(m_sqnf.group(1)) );
			if( m_sqnf.group(2).equals("") ) {
				break;
			}
			else {
				sqnf = m_sqnf.group(2).substring(1);
			}
		}
		
		initFromValues( name, columns, filterDefns, entityDefns, resultSetDefn );
	}
	
	public String definition() {
		StringBuilder defn = new StringBuilder(100000);
		String sep = "";
		
		defn.append(               "SpecLatticeNamedElement("                    );
		defn.append(                 "SpecLatticeQuery("                         );
		defn.append(                   "LatticeAddressSetPushforward("           );
		
		if( getFilterDefns().size() == 0 ) {
			defn.append(                 "LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty))");
			defn.append(               ", LatticeAddressSetMeet(empty)"          );
		}
		else {
			sep = "";
			StringBuilder tmplas = new StringBuilder(10000);
			tmplas.append(               "LatticeAddressSetMeet(("               );
			for( String f : getFilterDefns() ) {
				tmplas.append( sep );
				tmplas.append( f );
				sep = ", ";
			}
			tmplas.append(               "))"                                    );
			defn.append( String.format(  "LatticeAddressExpressionFromLAS(%s)",tmplas.toString()) );
			defn.append( String.format(", %s",tmplas.toString())                 );
		}
		
		if( getEntityDefns().size() == 0 ) {
			defn.append(               ", LatticeAddressExpressionMeet(empty)"   );
		}
		else {
			sep = "";
			defn.append(               ", LatticeAddressExpressionMeet(("        );
			for( String e : getEntityDefns() ) {
				defn.append( sep );
				defn.append( e );
				sep = ", ";
			}
			defn.append(                 "))"                                    );
		}
		
		defn.append(                   ")"                                       );
		defn.append(                 ", SpecQueryNamedFunctions("                );
		
		sep = "";
		for( QueryColumn c : getColumns() ) {
			defn.append( sep );
			defn.append( c.definition() );
			sep = " ";
		}
		
		defn.append(                   ")"                                       );
		defn.append(                 ", " + getResultSetDefn()                   );
		defn.append(                 ")"                                         );
		defn.append( String.format(", ContainerElementName(\"%s\")",getName())   );
		defn.append(               ")"                                           );
		
		return defn.toString();
	}
}
