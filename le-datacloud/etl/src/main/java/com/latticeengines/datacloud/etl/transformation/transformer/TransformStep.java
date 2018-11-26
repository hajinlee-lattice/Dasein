package com.latticeengines.datacloud.etl.transformation.transformer;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.metadata.Table;

public class TransformStep {

    private final String name;
    private String config;
    private Transformer transformer;
    protected Source[] baseSources;
    protected List<String> baseVersions;
    private Source[] baseTemplates;
    private Source target;
    private String targetVersion;
    private Source targetTemplate;
    private Schema targetSchema;
    private long elapsedTime;
    private Long count;
    private Map<String, Table> baseTables;

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

    public TransformStep(String name) {
        this.name = name;
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

    public void setTargetTemplate(Source targetTemplate) {
        this.targetTemplate = targetTemplate;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public Schema getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(Schema targetSchema) {
        this.targetSchema = targetSchema;
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

    public Map<String, Table> getBaseTables() {
        return baseTables;
    }

    public void setBaseTables(Map<String, Table> baseTables) {
        this.baseTables = baseTables;
    }

    public void setTarget(Source target) {
        this.target = target;
    }
}
