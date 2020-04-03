package com.latticeengines.domain.exposed.dcp.idaas;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RoleRequest {

    @JsonProperty("email_address")
    private String emailAddress;

    @JsonProperty("requestor")
    private String requestor;

    @JsonProperty("roles")
    private List<String> roles;

    @JsonProperty("products")
    private List<String> products;

    public String getEmailAddress() {
        return emailAddress;
    }

    public String getRequestor() {
        return requestor;
    }

    public List<String> getRoles() {
        return roles;
    }

    public List<String> getProducts() {
        return products;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public void setRequestor(String requestor) {
        this.requestor = requestor;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public void setProducts(List<String> products) {
        this.products = products;
    }
}
