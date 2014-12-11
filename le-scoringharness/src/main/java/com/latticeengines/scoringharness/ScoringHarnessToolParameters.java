package com.latticeengines.scoringharness;

import com.latticeengines.scoringharnessutilities.BaseAdminToolParameters;

public class ScoringHarnessToolParameters extends BaseAdminToolParameters {

	public static final String ARGS_GENERATE = "g";
	public static final String ARGS_HELP = "h";
	public static final String ARGS_LOAD = "l";
	
	public ScoringHarnessToolParameters(String[] args) {
		super(args);
	}

	public boolean getGenerate() {
		return this.getSwitch(ARGS_GENERATE);
	}
	
	public boolean getHelp() {
		return this.getSwitch(ARGS_HELP);
	}
	
	public boolean getLoad() {
		return this.getSwitch(ARGS_LOAD);
	}
	
	public String getLoadFile() {
		return this.getParameterValue(ARGS_LOAD);
	}

	public String toHelpString() {
		String toReturn = "ScoreHarnessTool [-" + ARGS_GENERATE + "] [-" + ARGS_HELP + "] [-" + ARGS_LOAD + " <file>]\n\n";
		toReturn += ARGS_GENERATE + " Generate a lead in Marketo\n";
		toReturn += ARGS_HELP + " Display help\n";
		toReturn += ARGS_LOAD + " Name of file to load\n";
		return toReturn;
	}

}
