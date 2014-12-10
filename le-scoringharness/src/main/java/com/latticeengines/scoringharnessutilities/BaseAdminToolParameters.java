package com.latticeengines.scoringharnessutilities;

import java.util.HashMap;

public abstract class BaseAdminToolParameters {
	
	public static final String OPTION_PREFIX = "-";
	
	private String optionPattern = "^" + OPTION_PREFIX + "[a-z|A-Z]{1}";
	private HashMap<String,String> parameters = new HashMap<String,String>();
	
	public String getParameterValue(String parameterName) {
		return parameters.get(parameterName);
	}
	
	public boolean getSwitch(String parameterName) {
		return parameters.containsKey(parameterName);
	}
	
	public static String formatOption(String optionName) {
		return OPTION_PREFIX + optionName;
	}
	
	public BaseAdminToolParameters(String[] args) {
		initializeParameters(args);
	}
	
	private void initializeParameters(String[] args) {
		String currentOptionName = null;
		for(String arg:args) {
			if(isOption(arg)) {
				currentOptionName = getOptionName(arg);
				parameters.put(currentOptionName, null);
			}
			else if(currentOptionName != null) {
				parameters.put(currentOptionName, arg);
				currentOptionName = null;
			}
		}
	}
	
	private boolean isOption(String arg) {
		if(arg.matches(optionPattern))
			return true;
		else
			return false;
	}
	
	private String getOptionName(String arg) {
		return arg.replaceFirst(OPTION_PREFIX, "");
	}
}
