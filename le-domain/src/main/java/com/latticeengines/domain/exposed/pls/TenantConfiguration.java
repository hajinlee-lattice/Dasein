package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TenantConfiguration {

    @JsonProperty("FeatureFlags")
    private FeatureFlagValueMap featureFlagValueMap;

    @JsonIgnore
    private List<LatticeProduct> products;


    public FeatureFlagValueMap getFeatureFlagValueMap() {
        return featureFlagValueMap;
    }

    public void setFeatureFlagValueMap(FeatureFlagValueMap featureFlagValueMap) {
        this.featureFlagValueMap = featureFlagValueMap;
    }

    @JsonIgnore
    public List<LatticeProduct> getProducts() {
        return products;
    }

    @JsonIgnore
    public void setProducts(List<LatticeProduct> products) {
        this.products = products;
    }

    @JsonProperty("Products")
    private List<String> getProductsAsStringList() {
        if (products == null) {
            return null;
        } else {
            return products.stream().map(LatticeProduct::getName).collect(Collectors.toList());
        }
    }

    @JsonProperty("Products")
    private void setProductsViaStringList(List<String> products) {
        if (products == null) {
            this.products = null;
        } else {
            this.products = products.stream().map(LatticeProduct::fromName).collect(Collectors.toList());
        }
    }

}
