package com.latticeengines.domain.exposed.dcp.idaas;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProductRequest {

    @JsonProperty("email_address")
    private String emailAddress;

    @JsonProperty("products")
    private List<String> products;

    @JsonProperty("requestor")
    private String requestor;

    public String getEmailAddress() {
        return emailAddress;
    }

    public List<String> getProducts() {
        return products;
    }

    public String getRequestor() {
        return requestor;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public void setProducts(List<String> products) {
        this.products = products;
    }

    public void setRequestor(String requestor) {
        this.requestor = requestor;
    }
}
