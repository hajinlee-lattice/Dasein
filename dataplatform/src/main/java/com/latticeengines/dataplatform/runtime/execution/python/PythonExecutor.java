package com.latticeengines.dataplatform.runtime.execution.python;


public class PythonExecutor {

	private static volatile PythonExecutor instance;
	
	public static PythonExecutor getInstance() {
		if (instance == null) {
			synchronized (PythonExecutor.class) {
				if (instance == null) {
					instance = new PythonExecutor();
				}
			}
		}
		return instance;
	}
	
	public void execute(String trainingData, String testData) {
		
	}
}
