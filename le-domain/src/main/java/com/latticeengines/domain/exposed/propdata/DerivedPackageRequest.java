package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DerivedPackageRequest {

    private String packageName;
    private String packageDescription;
    private Boolean isDefault = false;

    @JsonProperty("PackageName")
    public String getPackageName() {
        return packageName;
    }

    @JsonProperty("PackageName")
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    @JsonProperty("PackageDescription")
    public String getPackageDescription() {
        return packageDescription;
    }

    @JsonProperty("PackageDescription")
    public void setPackageDescription(String packageDescription) {
        this.packageDescription = packageDescription;
    }

    @JsonProperty("IsDefault")
    public Boolean getIsDefault() { return isDefault; }

    @JsonProperty("IsDefault")
    public void setIsDefault(Boolean isDefault) { this.isDefault = isDefault; }
}
