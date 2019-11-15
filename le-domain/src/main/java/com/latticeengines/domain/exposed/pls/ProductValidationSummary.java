package com.latticeengines.domain.exposed.pls;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductValidationSummary extends EntityValidationSummary {

    @JsonProperty("new_bundles")
    private Set<String> addedBundles;

    @JsonProperty("missing_bundles")
    private Set<String> missingBundles;

    @JsonProperty("processed_bundles")
    private Set<String> processedBundles;

    @JsonProperty("different_sku")
    private int differentSKU;

    @JsonProperty("missing_bundle_n_use")
    private int missingBundleInUse;

    public Set<String> getAddedBundles() {
        return addedBundles;
    }

    public void setAddedBundles(Set<String> addedBundles) {
        this.addedBundles = addedBundles;
    }

    public Set<String> getMissingBundles() {
        return missingBundles;
    }

    public void setMissingBundles(Set<String> missingBundles) {
        this.missingBundles = missingBundles;
    }

    public Set<String> getProcessedBundles() {
        return processedBundles;
    }

    public void setProcessedBundles(Set<String> processedBundles) {
        this.processedBundles = processedBundles;
    }

    public int getDifferentSKU() {
        return differentSKU;
    }

    public void setDifferentSKU(int differentSKU) {
        this.differentSKU = differentSKU;
    }

    public int getMissingBundleInUse() {
        return missingBundleInUse;
    }

    public void setMissingBundleInUse(int missingBundleInUse) {
        this.missingBundleInUse = missingBundleInUse;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
