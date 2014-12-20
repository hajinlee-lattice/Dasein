package com.latticeengines.data;

import java.util.*;

public class LEDataTable {
	private HashMap<String,Integer> columns = new HashMap<String,Integer>();
	private ArrayList<LEDataRow> rows = new ArrayList<LEDataRow>();
	
	public LEDataTable() {
		
	}
	
	public int getColumnCount() {
		return columns.size();
	}
	
	public void addColumn(String columnName) {
		columns.put(columnName, columns.size());
		
		for(LEDataRow row:rows) {
			row.addColumnValue();
		}
	}
	
	public int getColumnIndex(String columnName) {
		Integer toReturn = columns.get(columnName);
		if(toReturn == null)
			throw new IndexOutOfBoundsException("The column with name [" + columnName + "] does not exist in the table column collection.");
		return toReturn.intValue();
	}
	
	public LEDataRow addRow() {
		LEDataRow toReturn = new LEDataRow(this); 
		rows.add(toReturn);
		return toReturn;
	}
	
	public boolean columnExists(String columnName) {
		return columns.containsKey(columnName);
	}
}
