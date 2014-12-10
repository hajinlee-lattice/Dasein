package com.latticeengines.scoringharness;

import com.latticeengines.scoringharnessutilities.BaseAdminToolParameters;

public class ScoringHarnessToolParameters extends BaseAdminToolParameters {

	public static final String ARGS_LOAD = "l";
	public static final String ARGS_HELP = "h";
	
	public ScoringHarnessToolParameters(String[] args) {
		super(args);
	}

	public boolean getLoad() {
		return this.getSwitch(ARGS_LOAD);
	}
	
	public String getLoadFile() {
		return this.getParameterValue(ARGS_LOAD);
	}

	public boolean getHelp() {
		return this.getSwitch(ARGS_HELP);
	}
	
	public String toHelpString() {
		String toReturn = "ScoreHarnessTool [-" + ARGS_LOAD + " <file>] [-" + ARGS_HELP + "]\n\n";
		toReturn += ARGS_HELP + " Display help\n";
		toReturn += ARGS_LOAD + " Name of file to load\n";
		return toReturn;
	}

}
