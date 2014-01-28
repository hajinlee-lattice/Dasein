package com.latticeengines.dataplatform.runtime.execution.python;

import org.apache.hadoop.fs.Path;

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
	
	public void execute(Path trainingData, Path testData) {
		
	}
}
