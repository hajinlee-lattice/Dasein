package com.latticeengines.domain.exposed.metadata.transaction;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.latticeengines.common.exposed.util.HashUtils;

public class Product implements Serializable {
	private static final long serialVersionUID = 3364226218384157226L;
	
	private static final String UNKNOWN_PRODUCT_INTERNAL_ID = "c4ca1e5e-1018-4619-9f6b-bb23b7578b6a";
    private static final String PRODUCT_ID = "Id";
    private static final String PRODUCT_NAME = "ProductName";
    private static final String PRODUCT_DESCRIPTION = "Description";
    private static final String PRODUCT_TYPE = "ProductType";
//    private static final String PRODUCT_STATUS = "ProductStatus";
    private static final String PRODUCT_BUNDLE = "ProductBundle";
    private static final String PRODUCT_LINE = "ProductLine";
    private static final String PRODUCT_FAMILY = "ProductFamily";
    private static final String PRODUCT_CATEGORY = "ProductCategory";
    private static final String PRODUCT_BUNDLE_ID = "ProductBundleId";
    private static final String PRODUCT_LINE_ID = "ProductLineId";
    private static final String PRODUCT_FAMILY_ID = "ProductFamilyId";
    private static final String PRODUCT_CATEGORY_ID = "ProductCategoryId";

    private String id;
    private String name;
    private String description;
    private String type;
    private String bundle;
    private String line;
    private String family;
    private String category;
    private String status;
    private String bundleId;
    private String lineId;
    private String familyId;
    private String categoryId;

    public static final String UNKNOWN_PRODUCT_ID =
            HashUtils.getCleanedString(HashUtils.getShortHash(UNKNOWN_PRODUCT_INTERNAL_ID));

    public Product() {}

    public Product(String id, String name, String description, String type,
                   String bundle, String line, String family, String category,
                   String bundleId, String lineId, String familyId, String categoryId, String status) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.type = type;
        this.bundle = bundle;
        this.line = line;
        this.family = family;
        this.category = category;
        this.bundleId = bundleId;
        this.lineId = lineId;
        this.familyId = familyId;
        this.categoryId = categoryId;
        this.status = status;
    }

    @JsonIgnore
    public String getProductStatus() {
        return status;
    }

    @JsonIgnore
    public void setProductStatus(String status) {
        this.status = status;
    }

    @JsonProperty(Product.PRODUCT_ID)
    public String getProductId() {
        return id;
    }

    @JsonProperty(Product.PRODUCT_ID)
    public void setProductId(String productId) {
        this.id = productId;
    }

    @JsonProperty(Product.PRODUCT_NAME)
    public String getProductName() {
        return name;
    }

    @JsonProperty(Product.PRODUCT_NAME)
    public void setProductName(String productName) {
        this.name = productName;
    }

    @JsonProperty(Product.PRODUCT_DESCRIPTION)
    public String getProductDescription() {
        return description;
    }

    @JsonProperty(Product.PRODUCT_DESCRIPTION)
    public void setProductDescription(String description) {
        this.description = description;
    }

    @JsonProperty(Product.PRODUCT_TYPE)
    public String getProductType() {
        return type;
    }

    @JsonProperty(Product.PRODUCT_TYPE)
    public void setProductType(String type) {
        this.type = type;
    }

    @JsonProperty(Product.PRODUCT_BUNDLE)
    public String getProductBundle() {
        return bundle;
    }

    @JsonProperty(Product.PRODUCT_BUNDLE)
    public void setProductBundle(String bundleName) {
        this.bundle = bundleName;
    }

    @JsonProperty(Product.PRODUCT_LINE)
    public String getProductLine() {
        return line;
    }

    @JsonProperty(Product.PRODUCT_LINE)
    public void setProductLine(String productLine) {
        this.line = productLine;
    }

    @JsonProperty(Product.PRODUCT_FAMILY)
    public String getProductFamily() {
        return family;
    }

    @JsonProperty(Product.PRODUCT_FAMILY)
    public void setProductFamily(String productFamily) {
        this.family = productFamily;
    }

    @JsonProperty(Product.PRODUCT_CATEGORY)
    public String getProductCategory() {
        return category;
    }

    @JsonProperty(Product.PRODUCT_CATEGORY)
    public void setProductCategory(String productCategory) {
        this.category = productCategory;
    }

    @JsonProperty(Product.PRODUCT_BUNDLE_ID)
    public String getProductBundleId() {
        return bundleId;
    }

    @JsonProperty(Product.PRODUCT_BUNDLE_ID)
    public void setProductBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    @JsonProperty(Product.PRODUCT_LINE_ID)
    public String getProductLineId() {
        return lineId;
    }

    @JsonProperty(Product.PRODUCT_LINE_ID)
    public void setProductLineId(String lineId) {
        this.lineId = lineId;
    }

    @JsonProperty(Product.PRODUCT_FAMILY_ID)
    public String getProductFamilyId() {
        return familyId;
    }

    @JsonProperty(Product.PRODUCT_FAMILY_ID)
    public void setProductFamilyId(String familyId) {
        this.familyId = familyId;
    }

    @JsonProperty(Product.PRODUCT_CATEGORY_ID)
    public String getProductCategoryId() {
        return categoryId;
    }

    @JsonProperty(Product.PRODUCT_CATEGORY_ID)
    public void setProductCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }
}
