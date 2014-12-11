package com.latticeengines.scoringharness;

public class ScoringHarnessTool {
	
	private ScoringHarnessToolParameters parameters;
	
	public ScoringHarnessTool(String[] args) {
		parameters = new ScoringHarnessToolParameters(args);
	}
	
	public static void main(String[] args) throws Exception {
		ScoringHarnessTool tool = new ScoringHarnessTool(args);
		tool.execute();
	}

	private void validateParameters() {
		if(parameters.getHelp()) {
		} else if(parameters.getGenerate()) {
		} else if(parameters.getLoad()) {
			String file = parameters.getLoadFile();
			if(file == null || file.trim().isEmpty())
				throw new IllegalArgumentException("Must specify a file containing test data.");
		} else {
			throw new IllegalArgumentException("Must specify at least one parameter.");
		}
	}
	
	private void execute() throws Exception {
		try {
			System.out.println("Executing ScoringHarnessTool...");
			validateParameters();
			executeArguments();
			System.out.println("Completed execution of ScoringHarnessTool.");
		} catch(IllegalArgumentException e) {
			// Future: Log stack trace
			System.out.println("Error: " + e.getMessage());
			System.out.println(parameters.toHelpString());
		} catch(Exception e) {
			// Future: Log stack trace
			throw new Exception(e);
		}
	}
	
	private void executeArguments() {
		if(parameters.getHelp()) {
			System.out.println(parameters.toHelpString());
		} else if(parameters.getGenerate()) {
			executeLoad();
		} else if(parameters.getLoad()) {
			executeLoad();
		}
	}
	
	private void executeGenerate() {
		
	}
	
	private void executeLoad() {
		System.out.println("Load option is not currently supported.");
	}
}
