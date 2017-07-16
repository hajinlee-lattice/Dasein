package com.latticeengines.domain.exposed.camille.featureflags;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureFlagDefinition {

    @JsonProperty("DisplayName")
    private String displayName;

    @JsonProperty("Documentation")
    private String documentation;

    @JsonProperty("AvailableProducts")
    private Set<LatticeProduct> availableProducts;

    @JsonProperty("Configurable")
    private boolean configurable;

    @JsonProperty("ModifiableAfterProvisioning")
    private boolean modifiableAfterProvisioning = true;

    @JsonProperty("DefaultValue")
    private boolean defaultValue = false;

    @JsonProperty("Deprecated")
    private boolean deprecated = false;

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDocumentation() {
        return this.documentation;
    }

    public void setDocumentation(String documentation) {
        this.documentation = documentation;
    }

    public Set<LatticeProduct> getAvailableProducts() {
        return this.availableProducts;
    }

    public void setAvailableProducts(Set<LatticeProduct> availableProducts) {
        this.availableProducts = availableProducts;
    }

    public boolean getConfigurable() {
        return this.configurable;
    }

    public void setConfigurable(boolean configurable) {
        this.configurable = configurable;
    }

    public boolean isModifiableAfterProvisioning() {
        return modifiableAfterProvisioning;
    }

    public void setModifiableAfterProvisioning(boolean modifiableAfterProvisioning) {
        this.modifiableAfterProvisioning = modifiableAfterProvisioning;
    }

    public void setDefaultValue(boolean defaultValue) {
        this.defaultValue = defaultValue;
    }

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
