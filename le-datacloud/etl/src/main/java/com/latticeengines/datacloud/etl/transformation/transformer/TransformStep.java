package com.latticeengines.datacloud.etl.transformation.transformer;

import com.latticeengines.datacloud.core.source.Source;

import java.util.List;

public class TransformStep {

    private final String name;
    private String config;
    private Transformer transformer;
    private Source[] baseSources;
    private List<String> baseVersions;
    private Source[] baseTemplates;
    private Source target;
    private String targetVersion;
    private Source targetTemplate;
    private long elapsedTime;
    private Long count;

    public TransformStep(String name, Transformer transformer, Source[] baseSources, List<String> baseVersions,
                         Source[] baseTemplates, Source target, String targetVersion, Source targetTemplate, String config) {
        this.name = name;
        this.transformer = transformer;
        this.config = config;
        this.baseSources = baseSources;
        this.baseVersions = baseVersions;
        this.baseTemplates = baseTemplates;
        this.target = target;
        this.targetVersion = targetVersion;
        this.targetTemplate = targetTemplate;
    }

    public String getName() {
        return name;
    }

    public Transformer getTransformer() {
        return transformer;
    }

    public String getConfig() {
        return config;
    }

    public Source[] getBaseSources() {
        return baseSources;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public Source[] getBaseTemplates() {
        return baseTemplates;
    }

    public Source getTarget() {
        return target;
    }

    public Source getTargetTemplate() {
        return targetTemplate;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
