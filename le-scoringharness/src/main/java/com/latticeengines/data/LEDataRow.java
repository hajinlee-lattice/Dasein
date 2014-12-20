package com.latticeengines.data;

import java.util.ArrayList;

public class LEDataRow {
	private ArrayList<LEValue> columnValues = new ArrayList<LEValue>();
	private LEDataTable table = null;

	public LEDataRow(LEDataTable table)
	{
		this.table = table;
		for(int i = 0; i < table.getColumnCount(); i++) {
			columnValues.add(new LEValue());
		}
	}

	public void set(
			int columnIndex,
			String value) {
		columnValues.get(columnIndex).setValue(value);
	}
	
	public void set(
			String columnName,
			String value) {
		int columnIndex = table.getColumnIndex(columnName);
		set(columnIndex, value);
	}
	
	public String getString(int columnIndex) {
		return columnValues.get(columnIndex).getValueString();
	}
	
	public String getString(String columnName) {
		int columnIndex = table.getColumnIndex(columnName);
		return columnValues.get(columnIndex).getValueString();
	}
	
	public void addColumnValue() {
		columnValues.add(new LEValue());
	}
}
