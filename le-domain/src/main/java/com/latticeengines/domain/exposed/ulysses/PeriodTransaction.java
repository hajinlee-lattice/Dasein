package com.latticeengines.domain.exposed.ulysses;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PeriodTransaction {

    @JsonProperty("PeriodId")
    private Integer periodId;
    @JsonProperty("ProductId")
    private String productId;
    @JsonProperty("TotalAmount")
    private Double totalAmount;
    @JsonProperty("TotalQuantity")
    private Double totalQuantity;
    @JsonProperty("TransactionCount")
    private Double transactionCount;

    public PeriodTransaction() {
    }

    public Integer getPeriodId() {
        return this.periodId;
    }

    public void setPeriodId(Integer periodId) {
        this.periodId = periodId;
    }

    public String getProductId() {
        return this.productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getTotalAmount() {
        return this.totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Double getTotalQuantity() {
        return this.totalQuantity;
    }

    public void setTotalQuantity(Double totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    public Double getTransactionCount() {
        return this.transactionCount;
    }

    public void setTransactionCount(Double transactionCount) {
        this.transactionCount = transactionCount;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
