package com.latticeengines.scoringharness;

import com.latticeengines.scoringharnessutilities.BaseAdminToolParameters;

public class BaseAdminToolParametersTester extends BaseAdminToolParameters {

	public BaseAdminToolParametersTester(String[] args) {
		super(args);
	}
	
	public String getFile() {
		return this.getParameterValue("f");
	}

	public boolean getx() {
		return this.getSwitch("x");
	}

	public String getSomeOther() {
		return this.getParameterValue("s");
	}

	public int getNumber() throws IllegalArgumentException {
		String raw = this.getParameterValue("n");
		try {
			return Integer.parseInt(raw);
		} catch(NumberFormatException e) {
			throw new IllegalArgumentException("Must specify an integer value for -n argument.", e);
		}
	}

}
