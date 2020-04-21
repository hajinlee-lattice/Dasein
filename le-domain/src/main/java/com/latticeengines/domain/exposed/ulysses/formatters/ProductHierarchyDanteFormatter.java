package com.latticeengines.domain.exposed.ulysses.formatters;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;

@Component(ProductHierarchyDanteFormatter.Qualifier)
public class ProductHierarchyDanteFormatter implements DanteFormatter<ProductHierarchy> {
    public static final String Qualifier = "productHierarchyDanteFormatter";
    private static final String notionName = "DanteProductHierarchy";

    private class DanteProductHierarchy {
        private ProductHierarchy productHierarchy;

        private DanteProductHierarchy(ProductHierarchy productHierarchy) {
            this.productHierarchy = productHierarchy;
        }

        @JsonProperty(value = "BaseExternalID", index = 1)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getBaseExternalId() {
            return productHierarchy.getProductId();
        }

        @JsonProperty(value = "Level1Name", index = 8)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getLevel1Name() {
            return productHierarchy.getProductCategory();
        }

        @JsonProperty(value = "Level2Name", index = 3)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getLevel2Name() {
            return productHierarchy.getProductFamily();
        }

        @JsonProperty(value = "Level3Name", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getLevel3Name() {
            return productHierarchy.getProductLine();
        }

        @JsonProperty(value = "NotionName", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getNotionName() {
            return notionName;
        }

        @JsonProperty(value = "ProductHierarchyID", index = 6)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getProductHierarchyID() {
            return productHierarchy.getProductId();
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this, DanteFormatter.DanteFormat.class);
        }
    }

    @Override
    public String format(ProductHierarchy entity) {
        return new DanteProductHierarchy(entity).toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> format(List<ProductHierarchy> entities) {
        return entities != null //
                ? entities.stream().map(this::format).collect(Collectors.toList()) //
                : Collections.EMPTY_LIST;
    }
}
