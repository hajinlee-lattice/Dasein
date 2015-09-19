package com.latticeengines.domain.exposed.swlib;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class SoftwarePackage implements HasName {
    
    private String name;
    private String initializerClassName;
    private String module;
    private String groupId;
    private String artifactId;
    private String version;
    private String classifier = "";
    
    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("initializer_class")
    public String getInitializerClass() {
        return initializerClassName;
    }

    @JsonProperty("initializer_class")
    public void setInitializerClass(String initializerClassName) {
        this.initializerClassName = initializerClassName;
    }
    
    @JsonIgnore
    public String getHdfsPath() {
        return getHdfsPath("jar");
    }

    @JsonIgnore
    public String getHdfsPath(String extension) {
        assert(!StringUtils.isEmpty(module));
        assert(!StringUtils.isEmpty(groupId));
        assert(!StringUtils.isEmpty(artifactId));
        assert(!StringUtils.isEmpty(version));
        String fileName = String.format("%s-%s-%s.%s", artifactId, version, classifier, extension);
        if (StringUtils.isEmpty(classifier)) {
            fileName = String.format("%s-%s.%s", artifactId, version, extension);
        }
        
        String[] groupIdSplit = StringUtils.split(groupId, ".");
        String[] sSplit = new String[groupIdSplit.length];
        
        for (int i = 0; i < sSplit.length; i++) {
            sSplit[i] = "%s";
        }
        return String.format("%s/%s/%s/%s/%s", //
                module, StringUtils.join(groupIdSplit, "/"), artifactId, version, fileName);
    }

    @JsonProperty("module")
    public String getModule() {
        return module;
    }

    @JsonProperty("module")
    public void setModule(String module) {
        this.module = module;
    }

    @JsonProperty("group_id")
    public String getGroupId() {
        return groupId;
    }

    @JsonProperty("group_id")
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @JsonProperty("artifact_id")
    public String getArtifactId() {
        return artifactId;
    }

    @JsonProperty("artifact_id")
    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    @JsonProperty("version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty("classifier")
    public String getClassifier() {
        return classifier;
    }

    @JsonProperty("classifier")
    public void setClassifier(String classifier) {
        this.classifier = classifier;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
    
}
