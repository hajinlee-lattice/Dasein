package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResetPasswordRequest {

    private String username;
    private String hostPort;
    private LatticeProduct product;

    public ResetPasswordRequest() { }

    @JsonProperty("Username")
    public String getUsername() {
        return username;
    }

    @JsonProperty("Username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("HostPort")
    public String getHostPort() {
        return hostPort;
    }

    @JsonProperty("HostPort")
    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    @JsonProperty("Product")
    public LatticeProduct getProduct() {
        return product;
    }

    @JsonProperty("Product")
    public void setProduct(LatticeProduct product) {
        this.product = product;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
