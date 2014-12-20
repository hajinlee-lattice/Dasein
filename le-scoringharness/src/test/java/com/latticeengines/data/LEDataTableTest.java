package com.latticeengines.data;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.*;

public class LEDataTableTest extends TestCase {

	public LEDataTableTest(String name) {
		super(name);
	}

    public static Test suite() {
        return new TestSuite( LEDataTableTest.class );
    }
    
    public void testHashMap() {
    	HashMap<String, Integer> map = new HashMap<String, Integer>();
    	map.put("c", 0);
    	map.put("b", 1);
    	map.put("a", 2);
    	
    	HashMap<Integer, Integer> map2 = new HashMap<Integer, Integer>();
    	map2.put(2, 0);
    	map2.put(1, 1);
    	map2.put(0, 2);
    	
    }
    
    public void testLEDataTable() {
    	LEDataTable table = new LEDataTable();
    	table.addColumn("col1");
    	table.addColumn("col2");
    	table.addColumn("col3");
    	LEDataRow row = table.addRow();
    	row.set("col2", "2");
    	row.set("col1", "1");
    	row.set("col3", "3");
    	LEDataRow row2 = table.addRow();
    	row2.set("col1", "4");
    	row2.set("col2", "5");
    	row2.set("col3", "6");
    	table.addColumn("abc");
    	row.set("abc", "A");
    	row2.set("abc", "Z");
    	
    	assertTrue("Table value does not match expected value.", row.getString("col1") == "1");
    	assertTrue("Table value does not match expected value.", row.getString("col2") == "2");
    	assertTrue("Table value does not match expected value.", row.getString("col3") == "3");
    	assertTrue("Table value does not match expected value.", row.getString("abc") == "A");
    	assertTrue("Table value does not match expected value.", row2.getString("col1") == "4");
    	assertTrue("Table value does not match expected value.", row2.getString("col2") == "5");
    	assertTrue("Table value does not match expected value.", row2.getString("col3") == "6");
    	assertTrue("Table value does not match expected value.", row2.getString("abc") == "Z");
    }
}
