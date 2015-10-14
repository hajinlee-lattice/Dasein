package com.latticeengines.domain.exposed.modeling;

public enum SamplingType {

    DEFAULT_SAMPLING("DefaultSamplingJobCustomizer"), //
    BOOTSTRAP_SAMPLING("BootstrapSamplingJobCustomizer"), //
    STRATIFIED_SAMPLING("StratifiedSamplingJobCustomizer"), //
    UP_SAMPLING("UpSamplingJobCustomizer"), //
    DOWN_SAMPLING("DownSamplingJobCustomizer");

    private static final String CUSTOMOZER_PACKAGE_PATH = "com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer.";
    private final String customizerClassName;

    SamplingType(String customizerClassName) {
        this.customizerClassName = CUSTOMOZER_PACKAGE_PATH + customizerClassName;
    }

    public String getCustomizerClassName() {
        return this.customizerClassName;
    }
}