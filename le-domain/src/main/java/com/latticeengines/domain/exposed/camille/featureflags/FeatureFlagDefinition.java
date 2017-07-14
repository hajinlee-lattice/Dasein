package com.latticeengines.domain.exposed.camille.featureflags;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureFlagDefinition {
    private String displayName;
    private String documentation;
    private Set<LatticeProduct> availableProducts;
    private boolean configurable;
    private boolean modifiableAfterProvisioning = true;
    private boolean defaultValue = false;
    private boolean deprecated = false;

    @JsonProperty("DisplayName")
    public String getDisplayName() {
        return this.displayName;
    }

    @JsonProperty("DisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("Documentation")
    public String getDocumentation() {
        return this.documentation;
    }

    @JsonProperty("Documentation")
    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    @JsonProperty("AvailableProducts")
    public Set<LatticeProduct> getAvailableProducts() {
        return this.availableProducts;
    }

    @JsonProperty("AvailableProducts")
    public void setAvailableProducts(Set<LatticeProduct> availableProducts) {
        this.availableProducts = availableProducts;
    }

    @JsonProperty("Configurable")
    public boolean getConfigurable() {
        return this.configurable;
    }

    @JsonProperty("Configurable")
    public void setConfigurable(boolean configurable) {
        this.configurable = configurable;
    }

    @JsonProperty("ModifiableAfterProvisioning")
    public boolean isModifiableAfterProvisioning() {
        return modifiableAfterProvisioning;
    }

    @JsonProperty("ModifiableAfterProvisioning")
    public void setModifiableAfterProvisioning(boolean modifiableAfterProvisioning) {
        this.modifiableAfterProvisioning = modifiableAfterProvisioning;
    }

    @JsonProperty("DefaultValue")
    public void setDefaultValue(boolean defaultValue) {
        this.defaultValue = defaultValue;
    }

    @JsonProperty("DefaultValue")
    public boolean getDefaultValue() {
        return this.defaultValue;
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
