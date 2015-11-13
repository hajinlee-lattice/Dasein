package com.latticeengines.domain.exposed.camille.featureflags;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

public class FeatureFlagDefinition {
	private String displayName;
	private String documentation;
	private Set<LatticeProduct> availableProducts;
	private boolean configurable;

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

	@Override
	public String toString() {
		return JsonUtils.serialize(this);
	}

	public static void main(String[] args) {
		FeatureFlagDefinition f = new FeatureFlagDefinition();
		f.setDisplayName("f");
		f.setDocumentation("f");
		Set<LatticeProduct> s = new HashSet<LatticeProduct>();
		s.add(LatticeProduct.LPA);
		f.setAvailableProducts(s);
		f.setConfigurable(true);
		System.out.println(f);
	}
}
