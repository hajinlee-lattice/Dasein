package com.latticeengines.scoringharness;

import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BaseAdminToolParametersTest extends TestCase {

	public BaseAdminToolParametersTest(String testName) {
		super(testName);
	}

    public static Test suite() {
        return new TestSuite( BaseAdminToolParametersTest.class );
    }
    
    public void testBaseAdminToolParameters() {
    	ArrayList<String> input = new ArrayList<String>(); 
    	
    	String file = "C:\test\testfile.csv";
    	boolean x = true;
    	String someOther = "some other option value";
    	Integer number = 33;
    	
    	input.add("-f");
    	input.add(file);
    	input.add("-x");
    	input.add("-s");
    	input.add(someOther);
    	input.add("-n");
    	input.add(number.toString());
    	
    	BaseAdminToolParametersTester tester = new BaseAdminToolParametersTester(
    			input.toArray(new String[input.size()]));
    	
    	assertEquals("-f argument does not match.", file, tester.getFile());
    	assertEquals("-x switch does not match.", x, tester.getx());
    	assertEquals("-s argument does not match.", someOther, tester.getSomeOther());
    	try {
			assertEquals("-n argument does not match.", number.intValue(), tester.getNumber());
		} catch (IllegalArgumentException e) {
			assertTrue("-n argment is the wrong format.", false);
		}
    }

}
