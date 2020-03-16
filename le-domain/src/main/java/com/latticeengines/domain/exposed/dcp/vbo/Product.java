package com.latticeengines.domain.exposed.dcp.vbo;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Product {
    @JsonProperty("orderID")
    private String orderId;

    @JsonProperty("participatingPointID")
    private String participatingPointId;

    @JsonProperty("orderNumber")
    private String orderNumber;

    @JsonProperty("orderLineItemID")
    private String orderLineItemId;

    @JsonProperty("assetLineItemID")
    private String assetLineItemId;

    @JsonProperty("assetLineItemName")
    private String assetLineItemName;

    @JsonProperty("orderLineItemName")
    private String orderLineItemName;

    @JsonProperty("agreementID")
    private String agreementId;

    @JsonProperty("agreementNumber")
    private String agreementNumber;

    @JsonProperty("referenceID")
    private String referenceId;

    @JsonProperty("OverageAssetLineItemID1")
    private String OverageAssetLineItemId1;

    @JsonProperty("OverageAssetLineItemID2")
    private String OverageAssetLineItemId2;

    @JsonProperty("OverageAssetLineItemID3")
    private String OverageAssetLineItemId3;

    @JsonProperty("OvereageAssetlineItemName1")
    private String OvereageAssetlineItemName1;

    @JsonProperty("OvereageAssetlineItemName2")
    private String OvereageAssetlineItemName2;

    @JsonProperty("Overeage")
    private String Overeage;

    @JsonProperty("specification")
    private Specification specification;

    @JsonProperty("productDetail")
    private ProductDetail productDetail;

    @JsonProperty("users")
    private List<User> users;

    @JsonProperty("salesRep")
    private SalesRep salesRep;

    private class SalesRep {
        @JsonProperty("firstName")
        private String firstName;

        @JsonProperty("lastName")
        private String lastName;

        @JsonProperty("emailAddress")
        private String emailAddress;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getParticipatingPointId() {
        return participatingPointId;
    }

    public void setParticipatingPointId(String participatingPointId) {
        this.participatingPointId = participatingPointId;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public String getOrderLineItemId() {
        return orderLineItemId;
    }

    public void setOrderLineItemId(String orderLineItemId) {
        this.orderLineItemId = orderLineItemId;
    }

    public String getAssetLineItemId() {
        return assetLineItemId;
    }

    public void setAssetLineItemId(String assetLineItemId) {
        this.assetLineItemId = assetLineItemId;
    }

    public String getAssetLineItemName() {
        return assetLineItemName;
    }

    public void setAssetLineItemName(String assetLineItemName) {
        this.assetLineItemName = assetLineItemName;
    }

    public String getOrderLineItemName() {
        return orderLineItemName;
    }

    public void setOrderLineItemName(String orderLineItemName) {
        this.orderLineItemName = orderLineItemName;
    }

    public String getAgreementId() {
        return agreementId;
    }

    public void setAgreementId(String agreementId) {
        this.agreementId = agreementId;
    }

    public String getAgreementNumber() {
        return agreementNumber;
    }

    public void setAgreementNumber(String agreementNumber) {
        this.agreementNumber = agreementNumber;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public String getOverageAssetLineItemId1() {
        return OverageAssetLineItemId1;
    }

    public void setOverageAssetLineItemId1(String overageAssetLineItemId1) {
        OverageAssetLineItemId1 = overageAssetLineItemId1;
    }

    public String getOverageAssetLineItemId2() {
        return OverageAssetLineItemId2;
    }

    public void setOverageAssetLineItemId2(String overageAssetLineItemId2) {
        OverageAssetLineItemId2 = overageAssetLineItemId2;
    }

    public String getOverageAssetLineItemId3() {
        return OverageAssetLineItemId3;
    }

    public void setOverageAssetLineItemId3(String overageAssetLineItemId3) {
        OverageAssetLineItemId3 = overageAssetLineItemId3;
    }

    public String getOvereageAssetlineItemName1() {
        return OvereageAssetlineItemName1;
    }

    public void setOvereageAssetlineItemName1(String overeageAssetlineItemName1) {
        OvereageAssetlineItemName1 = overeageAssetlineItemName1;
    }

    public String getOvereageAssetlineItemName2() {
        return OvereageAssetlineItemName2;
    }

    public void setOvereageAssetlineItemName2(String overeageAssetlineItemName2) {
        OvereageAssetlineItemName2 = overeageAssetlineItemName2;
    }

    public String getOvereage() {
        return Overeage;
    }

    public void setOvereage(String overeage) {
        Overeage = overeage;
    }

    public Specification getSpecification() {
        return specification;
    }

    public void setSpecification(Specification specification) {
        this.specification = specification;
    }

    public ProductDetail getProductDetail() {
        return productDetail;
    }

    public void setProductDetail(ProductDetail productDetail) {
        this.productDetail = productDetail;
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public SalesRep getSalesRep() {
        return salesRep;
    }

    public void setSalesRep(SalesRep salesRep) {
        this.salesRep = salesRep;
    }

    private class Specification {
        @JsonProperty("productID")
        private String productId;

        @JsonProperty("sellingRegion")
        private String sellingRegion;

        @JsonProperty("additionalParameters")
        private List<additionalParameter> additionalParameters;

        private class additionalParameter {
            @JsonProperty("key")
            private String key;

            @JsonProperty("value")
            private String value;
        }
    }

    private class ProductDetail {
        @JsonProperty("changeEvent")
        private String changeEvent;

        @JsonProperty("startDateTime")
        private String startDateTime;

        @JsonProperty("endDateTime")
        private String endDateTime;
    }
}
