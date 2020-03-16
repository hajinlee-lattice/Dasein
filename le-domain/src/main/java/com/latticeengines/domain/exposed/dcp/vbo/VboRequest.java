package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

@JsonInclude(Include.NON_NULL)
public class VboRequest {

    @JsonProperty("subscriber")
    @ApiModelProperty(required = true, value = "subscriber")
    private Subscriber subscriber;

    @JsonProperty("product")
    @ApiModelProperty(required = false, value = "product")
    private Product product;

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public void setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product projectId) {
        this.product = product;
    }

}
