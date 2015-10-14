package com.latticeengines.dataplatform.runtime.python;

public enum PythonMRJobType {
    PROFILING_JOB("profiling", "profilingConfig.txt"), //
    MODELING_JOB("modeling", "modelingConfig.txt");

    public static final String CONFIG_FILE = ".*Config.txt";

    private final String jobName;
    private final String configName;

    PythonMRJobType(String jobName, String configName) {
        this.jobName = jobName;
        this.configName = configName;
    }

    public String jobType() {
        return this.jobName;
    }

    public String configName() {
        return this.configName;
    }
}
