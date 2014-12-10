package com.latticeengines.scoringharness;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ScoringHarnessToolTest extends TestCase {

	public ScoringHarnessToolTest(String name) {
		super(name);
	}

    public static Test suite() {
        return new TestSuite( ScoringHarnessToolTest.class );
    }
    
	public void testHelp() throws Exception {
		ScoringHarnessTool.main(new String[]{
				ScoringHarnessToolParameters.formatOption(ScoringHarnessToolParameters.ARGS_HELP)});
	}

	public void testLoad() throws Exception {
		ScoringHarnessTool.main(new String[]{
				ScoringHarnessToolParameters.formatOption(ScoringHarnessToolParameters.ARGS_LOAD),
				"testLoad.csv"});
	}
}
