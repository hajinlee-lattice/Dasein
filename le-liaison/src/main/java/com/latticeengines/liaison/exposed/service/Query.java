package com.latticeengines.liaison.exposed.service;

import com.latticeengines.liaison.exposed.exception.DefinitionException;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import java.lang.RuntimeException;
import java.util.ArrayList;
import java.util.List;

public abstract class Query {
	
	private String name;
	private List<QueryColumn> columns;
	private List<String> columnNames;
	private List<String> filterDefns;
	private List<String> entityDefns;
	private String resultSetDefn;
	
	public Query( String name, String definition ) throws DefinitionException {
		initFromDefinition( name, definition );
	}
	
	public Query( String name, List<QueryColumn> columns, List<String> filterDefns, List<String> entityDefns, String resultSetDefn ) {
		initFromValues( name, columns, filterDefns, entityDefns, resultSetDefn );
	}
	
	public Query( Query other ) {
		initFromValues( other.name, other.columns, other.filterDefns, other.entityDefns, other.resultSetDefn );
	}
	
	public String getName() {
		return name;
	}
	
	public void setName( String name ) {
		this.name = name;
	}
	
	public List<QueryColumn> getColumns() {
		return columns;
	}
	
	public List<String> getColumnNames() {
		return columnNames;
	}
	
	public List<String> getFilterDefns() {
		return filterDefns;
	}
	
	public List<String> getEntityDefns() {
		return entityDefns;
	}
	
	public String getResultSetDefn() {
		return resultSetDefn;
	}
	
	public QueryColumn getColumn( String colname ) throws RuntimeException {
		if( !columnNames.contains(colname) ) {
			throw new RuntimeException( String.format("Column \"%s\" does not exist in query \"%s\"",colname,getName()) );
		}
		int idx = columnNames.indexOf( colname );
		return columns.get( idx );
	}
	
	public void appendColumn( QueryColumn column ) {
		if( !columnNames.contains(column.getName()) ) {
			columns.add( createColumn(column) );
			columnNames.add( column.getName() );
		}
	}
	
	public void removeColumn( String colname ) {
		if( columnNames.contains(colname) ) {
			int idx = columnNames.indexOf( colname );
			columnNames.remove( idx );
			columns.remove( idx );
		}
	}
	
	public void updateColumn( QueryColumn column ) {
		if( columnNames.contains(column.getName()) ) {
			int idx = columnNames.indexOf( column.getName() );
			columns.get( idx ).initFromValues( column.getName(), column.getExpression(), column.getMetadata() );
		}
		else {
			appendColumn( column );
		}
	}
	
	public void initFromValues( String name, List<QueryColumn> columns, List<String> filterDefns, List<String> entityDefns, String resultSetDefn ) {
		this.name = name;
		this.columns = new ArrayList<>();
		this.columnNames = new ArrayList<>();
		this.filterDefns = new ArrayList<>(filterDefns);
		this.entityDefns = new ArrayList<>(entityDefns);
		this.resultSetDefn = resultSetDefn;
		for( QueryColumn qc : columns ) {
			this.columns.add( createColumn(qc) );
			this.columnNames.add( qc.getName() );
		}
	}
	
	public abstract QueryColumn createColumn( QueryColumn qc );
	
	public abstract void initFromDefinition( String name, String defn ) throws DefinitionException;
	
	public abstract String definition();
	
}
