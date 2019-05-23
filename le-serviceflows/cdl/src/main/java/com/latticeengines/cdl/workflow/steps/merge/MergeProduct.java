package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

@Component(MergeProduct.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProduct extends BaseSingleEntityMergeImports<ProcessProductStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MergeProduct.class);

    static final String BEAN_NAME = "mergeProduct";
    private Map<String, Object> mergeReport;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeProduct");
            TransformationStepConfig merge = mergeInputs(true, false, true);
            List<TransformationStepConfig> steps = Collections.singletonList(merge);
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        Table table = createMergedConsolidatedProductTable();

        Table inputTable = metadataProxy.getTable(customerSpace.toString(),
                TableUtils.getFullTableName(mergedBatchStoreName, pipelineVersion));
        List<Product> inputProducts = ProductUtils.loadProducts(yarnConfiguration,
                inputTable.getExtracts().get(0).getPath(), null, null);

        Table currentTable = getCurrentConsolidateProductTable();
        List<Product> currentProducts = getCurrentProducts(currentTable);

        Map<String, Integer> productCounts = countProducts(currentProducts);
        log.info("product count is " + JsonUtils.serialize(productCounts));
        mergeReport = constructMergeReport(productCounts, currentProducts.size());

        List<Product> productList = new ArrayList<>();
        int nInvalids = 0;
        try {
            nInvalids = mergeProducts(inputProducts, currentProducts, productList, mergeReport);
        } catch (Exception exc) {
            productList = currentProducts;
            String errMsg = "Found inconsistency during imports. Current product map will be used. ";
            log.error(errMsg + exc.getMessage(), exc);
            mergeReport.put("Merged_NumProductBundles", 0);
            mergeReport.put("Merged_NumProductHierarchies", 0);
            mergeReport.put("Merged_NumProductCategories", 0);
        }

        productCounts = countProducts(productList);
        Long productBundlesQuotaLimit = getLongValueFromContext(PRODUCT_BUNDLES_DATAQUOTA_LIMIT);
        if ( productBundlesQuotaLimit < productCounts.get("nProductAnalytics"))
            throw new IllegalStateException(
                    "the Analytics Product data quota limit is " + productBundlesQuotaLimit
                            + ", The data you uploaded has exceeded the limit.");
        Long productSkuQuotaLimit = getLongValueFromContext(PRODUCT_SKU_DATAQUOTA_LIMIT);
        if (productSkuQuotaLimit < productCounts.get("nProductSpendings"))
            throw new IllegalStateException(
                    "the Spending Product data quota limit is " + productSkuQuotaLimit
                            + ", The data you uploaded has exceeded the limit.");
        updateMergeReport(inputProducts.size(), nInvalids, productList.size(), productCounts);
        updateEntityValueMapInContext(FINAL_RECORDS, (Integer) mergeReport.get("Merged_NumProductAnalytics"),
                Integer.class);
        updateEntityValueMapInContext(BusinessEntity.ProductHierarchy, FINAL_RECORDS,
                (Integer) mergeReport.get("Merged_NumProductHierarchies"), Integer.class);

        try {
            ProductUtils.saveProducts(yarnConfiguration, table.getExtracts().get(0).getPath(), productList);
        } catch (Exception exc) {
            log.error("Failed to save merged products to table " + table.getName());
        }

        // upsert product table
        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        log.info(String.format("Upsert table %s to role %s, version %s.", table.getName(),
                TableRoleInCollection.ConsolidatedProduct, inactive));

        generateReport();
    }

    private void generateReport() {
        // get product count in batch store
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                inactive);
        String hdfsPath = table.getExtracts().get(0).getPath();
        log.info(String.format("Count products in batch store. Tenant=%s. Version=%s. HDFSPath=%s",
                customerSpace.toString(), inactive, hdfsPath));
        long skus = ProductUtils
                .countProducts(yarnConfiguration, hdfsPath,
                        Arrays.asList(ProductType.Bundle.name(), ProductType.Hierarchy.name()));

        // update product report
        ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                ObjectNode.class);
        JsonNode entitiesSummaryNode = report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());
        if (entitiesSummaryNode == null) {
            entitiesSummaryNode = report.putObject(ReportPurpose.ENTITIES_SUMMARY.getKey());
        }
        JsonNode entityNode = entitiesSummaryNode.get(entity.name());
        if (entityNode == null) {
            entityNode = ((ObjectNode) entitiesSummaryNode).putObject(entity.name());
        }
        JsonNode consolidateSummaryNode = entityNode.get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
        if (consolidateSummaryNode == null) {
            consolidateSummaryNode = ((ObjectNode) entityNode)
                    .putObject(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
        }

        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_ID, skus);
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_HIERARCHY,
                (Integer) mergeReport.get("Merged_NumProductCategories"));
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.PRODUCT_BUNDLE,
                (Integer) mergeReport.get("Merged_NumProductBundles"));
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.ERROR_MESSAGE,
                (String) mergeReport.get("Merged_ErrorMessage"));
        ((ObjectNode) consolidateSummaryNode).put(ReportConstants.WARN_MESSAGE,
                (String) mergeReport.get("Merged_WarnMessage"));

        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
    }

    public int mergeProducts(List<Product> inputProducts, List<Product> currentProducts, List<Product> result,
            Map<String, Object> mergeReport) {
        boolean foundProductBundle = false;
        boolean foundProductHierarchy = false;
        int nInvalids = 0;
        Map<String, Product> inputProductMap = new HashMap<>();
        Map<String, Product> currentProductMap = ProductUtils.getProductMapByCompositeId(currentProducts);

        for (Product inputProduct : inputProducts) {
            if (inputProduct.getProductId() == null) {
                nInvalids++;
                continue;
            }

            if (inputProduct.getProductBundle() != null) {
                foundProductBundle = true;
                Product analyticProduct = mergeAnalyticProduct(null, inputProduct.getProductBundle(),
                        inputProduct.getProductBundle(), inputProduct.getProductDescription(), currentProductMap,
                        inputProductMap, mergeReport);
                String bundleId = analyticProduct.getProductId();
                inputProduct.setProductBundleId(bundleId);
                mergeBundleProduct(inputProduct, inputProductMap);
            }

            if (inputProduct.getProductCategory() != null) {
                foundProductHierarchy = true;
                String category = inputProduct.getProductCategory();
                String family = inputProduct.getProductFamily();
                String line = inputProduct.getProductLine();
                String categoryId;
                String familyId = null;
                String lineId;
                Product categoryProduct = mergeSpendingProduct(null, inputProduct.getProductCategory(), null, null,
                        null, category, null, null, currentProductMap, inputProductMap);
                categoryId = categoryProduct.getProductId();
                inputProduct.setProductCategoryId(categoryId);

                if (inputProduct.getProductFamily() != null) {
                    Product familyProduct = mergeSpendingProduct(null, inputProduct.getProductFamily(), categoryId,
                            null, null, category, family, null, currentProductMap, inputProductMap);
                    familyId = familyProduct.getProductId();
                    inputProduct.setProductFamilyId(familyId);
                }

                if (inputProduct.getProductLine() != null) {
                    Product lineProduct = mergeSpendingProduct(null, inputProduct.getProductLine(), categoryId,
                            familyId, null, category, family, line, currentProductMap, inputProductMap);
                    lineId = lineProduct.getProductId();
                    inputProduct.setProductLineId(lineId);
                }

                mergeHierarchyProduct(inputProduct, inputProductMap, mergeReport);
            }

            if (inputProduct.getProductBundle() == null && inputProduct.getProductCategory() == null) {
                if (inputProduct.getProductName() == null) {
                    String errMsg = "Product name is missing for product with id=" + inputProduct.getProductId();
                    mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                    throw new RuntimeException("Invalid product name for ProductId=" + inputProduct.getProductId());
                }
                // ProductId will be used in avro schema in curated metrics.
                // Need to check validation
                if (!AvroUtils.isValidColumn(inputProduct.getProductId())) {
                    String errMsg = String.format("Product has invalid id=%s", inputProduct.getProductId());
                    mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                    throw new RuntimeException(errMsg);
                }

                foundProductBundle = true;
                mergeAnalyticProduct(inputProduct.getProductId(), inputProduct.getProductName(),
                        inputProduct.getProductName(), inputProduct.getProductDescription(), currentProductMap,
                        inputProductMap, mergeReport);
            }
        }

        if (!foundProductBundle) {
            currentProductMap.forEach((compositeId, currentProduct) -> {
                if (currentProduct.getProductStatus().equals(ProductStatus.Active.name())
                        && (currentProduct.getProductType().equals(ProductType.Bundle.name())
                                || currentProduct.getProductType().equals(ProductType.Analytic.name()))) {
                    inputProductMap.put(compositeId, currentProduct);
                }
            });
        }

        if (!foundProductHierarchy) {
            currentProductMap.forEach((compositeId, currentProduct) -> {
                if (currentProduct.getProductStatus().equals(ProductStatus.Active.name())
                        && (currentProduct.getProductType().equals(ProductType.Hierarchy.name())
                                || currentProduct.getProductType().equals(ProductType.Spending.name()))) {
                    inputProductMap.put(compositeId, currentProduct);
                }
            });
        }

        for (Product product : inputProductMap.values()) {
            product.setProductStatus(ProductStatus.Active.name());
        }

        currentProductMap.forEach((compositeId, product) -> {
            if (!inputProductMap.keySet().contains(compositeId)) {
                product.setProductStatus(ProductStatus.Obsolete.name());
                inputProductMap.put(compositeId, product);
            }
        });

        result.addAll(inputProductMap.values());
        return nInvalids;
    }

    private Product mergeAnalyticProduct(String id, String name, String bundleName, String description,
            Map<String, Product> currentProductMap, Map<String, Product> inputProductMap,
            Map<String, Object> mergeReport) {
        String compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name(), id, name, bundleName, null, null,
                null);
        String productId = id;

        if (id == null) {
            productId = createProductId(compositeId, currentProductMap);
        }

        compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name(), productId, name, bundleName, null, null,
                null);
        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            log.info(String.format("Found product [productId=%s, compositeId=%s] in inputProductMap.",
                    product.getProductId(), compositeId));

            if (!product.getProductType().equals(ProductType.Analytic.name())) {
                String errMsg = String.format("Found inconsistent product type with bundle %s", bundleName);
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(String.format("Failed to merge analytic product. Id=%s, name=%s", id, name));
            }

            if (StringUtils.compare(product.getProductDescription(), description) != 0) {
                String warnMsg = String.format("Found inconsistent product description with bundle %s.", bundleName);
                mergeReport.putIfAbsent("Merged_WarnMessage", warnMsg);
            }
        } else {
            log.info(String.format("CompositeId=%s is not in inputProductMap. Create analytic product "
                    + "[productId=%s, compositeId=%s].", compositeId, productId, compositeId));
            Product newProduct = new Product();
            newProduct.setProductId(productId);
            newProduct.setProductName(name);
            newProduct.setProductBundle(bundleName);
            newProduct.setProductType(ProductType.Analytic.name());
            inputProductMap.put(compositeId, newProduct);
            product = newProduct;
        }

        return product;
    }

    private void mergeBundleProduct(Product inputProduct, Map<String, Product> inputProductMap) {
        String id = inputProduct.getProductId();
        String name = inputProduct.getProductName();
        String description = inputProduct.getProductDescription();
        String bundle = inputProduct.getProductBundle();
        String compositeId = ProductUtils.getCompositeId(ProductType.Bundle.name(), id, name, bundle, null, null, null);

        log.info(String.format("Create bundle product [productId=%s, bundle=%s, description=%s], bundleId=%s.", id,
                bundle, description, inputProduct.getProductBundleId()));
        Product newProduct = new Product();
        newProduct.setProductId(id);
        newProduct.setProductName(name);
        newProduct.setProductDescription(description);
        newProduct.setProductBundle(bundle);
        newProduct.setProductBundleId(inputProduct.getProductBundleId());
        newProduct.setProductType(ProductType.Bundle.name());
        inputProductMap.put(compositeId, newProduct);
    }

    private Product mergeSpendingProduct(String id, String name, String categoryId, String familyId, String lineId,
            String category, String family, String line, Map<String, Product> currentProductMap,
            Map<String, Product> inputProductMap) {
        String compositeId = ProductUtils.getCompositeId(ProductType.Spending.name(), id, name, null, category, family,
                line);
        String productId = id;

        if (id == null) {
            productId = createProductId(compositeId, currentProductMap);
        }

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            log.info(String.format("Found product in inputProductMap. Id=%s, compositeId=%s", product.getProductId(),
                    compositeId));
        } else {
            log.info(String.format("Create spending product [productId=%s, line=%s, family=%s, category=%s, "
                    + "lineId=%s, familyId=%s, categoryId=%s].",
                    productId, line, family, category, lineId, familyId, categoryId));
            Product newProduct = new Product();
            newProduct.setProductId(productId);
            newProduct.setProductName(name);
            newProduct.setProductCategory(category);
            newProduct.setProductFamily(family);
            newProduct.setProductLine(line);
            newProduct.setProductCategoryId(categoryId);
            newProduct.setProductFamilyId(familyId);
            newProduct.setProductLineId(lineId);
            newProduct.setProductType(ProductType.Spending.name());
            inputProductMap.put(compositeId, newProduct);
            product = newProduct;
        }

        return product;
    }

    private void mergeHierarchyProduct(Product inputProduct, Map<String, Product> inputProductMap,
            Map<String, Object> mergeReport) {
        String id = inputProduct.getProductId();
        String name = inputProduct.getProductName();
        String description = inputProduct.getProductDescription();
        String bundle = inputProduct.getProductBundle();
        String line = inputProduct.getProductLine();
        String lineId = inputProduct.getProductLineId();
        String familyId = inputProduct.getProductFamilyId();
        String family = inputProduct.getProductFamily();
        String categoryId = inputProduct.getProductCategoryId();
        String category = inputProduct.getProductCategory();
        String compositeId = ProductUtils.getCompositeId(ProductType.Hierarchy.name(), id, name, bundle, category,
                family, line);

        Product p = inputProductMap.get(compositeId);
        if (p != null) {
            if (StringUtils.compare(line, p.getProductLine()) != 0) {
                String errMsg = String.format(
                        "Product with same SKU [SKU=%s] has different product lines " + "[Line1=%s, Line2=%s].", id,
                        line, p.getProductLine());
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(errMsg);
            }

            if (StringUtils.compare(family, p.getProductFamily()) != 0) {
                String errMsg = String.format(
                        "Product with same SKU [SKU=%s] has different product families " + "[Family1=%s, Family2=%s].",
                        id, family, p.getProductFamily());
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(errMsg);
            }

            if (StringUtils.compare(category, p.getProductCategory()) != 0) {
                String errMsg = String.format("Product with same SKU [SKU=%s] has different product categories "
                        + "[Category1=%s, Category2=%s].", id, category, p.getProductCategory());
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(errMsg);
            }
        }

        if (StringUtils.isNotBlank(lineId)) {
            if (StringUtils.isBlank(familyId) || StringUtils.isBlank(categoryId)) {
                String errMsg = String.format(
                        "Product hierarchy has level-3 but does not have level-2 or level-1. ProductId=%s, ProductName=%s",
                        id, name);
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(
                        String.format("Product hierarchy has level-3 but does not have level-2 or level-1. Product=%s",
                                JsonUtils.serialize(inputProduct)));
            }
        } else if (StringUtils.isNotBlank(familyId)) {
            if (StringUtils.isBlank(categoryId)) {
                String errMsg = String.format(
                        "Product hierarchy has level-2 but does not have level-1. ProductId=%s, ProductName=%s", id,
                        name);
                mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
                throw new RuntimeException(
                        String.format("Product hierarchy has level-2 but does not have level-1. Product=%s",
                                JsonUtils.serialize(inputProduct)));
            }
        } else if (StringUtils.isBlank(categoryId)) {
            String errMsg = String.format("Product hierarchy does not have level-1. ProductId=%s, ProductName=%s", id,
                    name);
            mergeReport.putIfAbsent("Merged_ErrorMessage", errMsg);
            throw new RuntimeException(String.format("Product hierarchy does not have level-1. Product=%s",
                    JsonUtils.serialize(inputProduct)));
        }

        log.info(String.format(
                "Create hierarchical product [productId=%s, line=%s, family=%s, category=%s, "
                        + "lineId=%s, familyId=%s, categoryId=%s].",
                id, inputProduct.getProductLine(), inputProduct.getProductFamily(), inputProduct.getProductCategory(),
                lineId, familyId, categoryId));
        Product newProduct = new Product();
        newProduct.setProductId(id);
        newProduct.setProductName(name);
        newProduct.setProductDescription(description);
        newProduct.setProductLine(inputProduct.getProductLine());
        newProduct.setProductLineId(lineId);
        newProduct.setProductFamily(inputProduct.getProductFamily());
        newProduct.setProductFamilyId(familyId);
        newProduct.setProductCategory(inputProduct.getProductCategory());
        newProduct.setProductCategoryId(categoryId);
        newProduct.setProductType(ProductType.Hierarchy.name());
        inputProductMap.put(compositeId, newProduct);
    }

    public Map<String, Integer> countProducts(List<Product> products) {
        Set<String> productIdSet = new HashSet<>();
        Set<String> productHierarchySet = new HashSet<>();
        Set<String> productCategorySet = new HashSet<>();
        Set<String> productBundleSet = new HashSet<>();
        Set<String> productAnalyticSet = new HashSet<>();
        Set<String> productSpendingSet = new HashSet<>();
        Set<String> obsoleteProducts = new HashSet<>();
        Map<String, Integer> result = new HashMap<>();

        products.forEach(product -> {
            if (product.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                obsoleteProducts.add(product.getProductId());
            } else {
                if (product.getProductType().equals(ProductType.Bundle.name())) {
                    productIdSet.add(product.getProductId());
                    productBundleSet.add(product.getProductBundleId());
                }

                if (product.getProductType().equals(ProductType.Hierarchy.name())) {
                    productIdSet.add(product.getProductId());
                    productHierarchySet.add(product.getProductId());
                    productCategorySet.add(product.getProductCategoryId());
                }

                if (product.getProductType().equals(ProductType.Analytic.name())) {
                    productAnalyticSet.add(product.getProductId());
                }

                if (product.getProductType().equals(ProductType.Spending.name())) {
                    productSpendingSet.add(product.getProductId());
                }
            }
        });

        result.put("nProductIds", productIdSet.size());
        result.put("nProductBundles", productBundleSet.size());
        result.put("nProductHierarchies", productHierarchySet.size());
        result.put("nProductCategories", productCategorySet.size());
        result.put("nProductAnalytics", productAnalyticSet.size());
        result.put("nProductSpendings", productSpendingSet.size());
        result.put("nObsoleteProducts", obsoleteProducts.size());
        return result;
    }

    private String createProductId(String compositeId, Map<String, Product> currentProductMap) {
        Product currentProduct = currentProductMap.get(compositeId);
        if (currentProduct != null) {
            log.info(String.format("Found [compositeId=%s, productId=%s] in currentProductMap.", compositeId,
                    currentProduct.getProductId()));
            return currentProduct.getProductId();
        } else {
            log.info(String.format("Generating hashed productId by [productName=%s].", compositeId));
            return HashUtils.getCleanedString(HashUtils.getShortHash(compositeId));
        }
    }

    private Table createMergedConsolidatedProductTable() {
        Table table = SchemaRepository.instance().getSchema(SchemaInterpretation.Product, true);
        String fullTableName = TableUtils.getFullTableName(batchStore.name(), pipelineVersion);
        table.setName(fullTableName);
        table.setDisplayName(fullTableName);
        String dataTablePath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "")
                .toString();
        String hdfsPath = dataTablePath + "/" + fullTableName;
        try {
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
            log.info("Initialized merged product table " + hdfsPath);
        } catch (Exception exc) {
            log.error("Failed to initialize merged product table " + hdfsPath);
            throw new RuntimeException("Failed to create merged product table.");
        }
        Extract extract = new Extract();
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setName("extract_target");
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/*.avro");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);

        return table;
    }

    private Table getCurrentConsolidateProductTable() {
        Table currentTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, active);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + active);
            return currentTable;
        }

        currentTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                inactive);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + inactive);
            return currentTable;
        }

        log.info("There is no ConsolidatedProduct table with version " + active + " and " + inactive);
        return null;
    }

    private List<Product> getCurrentProducts(Table currentConsolidateProductTable) {
        List<Product> currentProducts;
        if (currentConsolidateProductTable != null) {
            currentProducts = ProductUtils.loadProducts(yarnConfiguration,
                    currentConsolidateProductTable.getExtracts().get(0).getPath(), null, null);
            currentProducts.forEach(product -> {
                if (product.getProductType() == null) {
                    log.info("Found null product type. ProductId=" + product.getProductId());
                    product.setProductType(ProductType.Analytic.name());
                }

                if (product.getProductStatus() == null) {
                    log.info("Found null product status. ProductId=" + product.getProductId());
                    product.setProductStatus(ProductStatus.Active.name());
                }
            });
        } else {
            currentProducts = new ArrayList<>();
        }
        return currentProducts;
    }

    public Map<String, Object> constructMergeReport(Map<String, Integer> productCounts, Integer currentProductsSize) {
        Map<String, Object> mergeReport = new HashMap<>();
        mergeReport.put("Current_NumProductsInTotal", currentProductsSize);
        mergeReport.put("Current_NumProductIds", productCounts.get("nProductIds"));
        mergeReport.put("Current_NumProductBundles", productCounts.get("nProductBundles"));
        mergeReport.put("Current_NumProductHierarchies", productCounts.get("nProductHierarchies"));
        mergeReport.put("Current_NumProductCategories", productCounts.get("nProductCategories"));
        mergeReport.put("Current_NumProductAnalytics", productCounts.get("nProductAnalytics"));
        mergeReport.put("Current_NumProductSpendings", productCounts.get("nProductSpendings"));
        mergeReport.put("Current_NumObsoleteProducts", productCounts.get("nObsoleteProducts"));

        log.info(String.format("Current product table has %s products in total, %s unique product SKUs, "
                + "%s product bundles, %s product hierarchies, %s product categories, %s analytic products, "
                + "%s spending products, %s obsolete products.",
                mergeReport.get("Current_NumProductsInTotal"), mergeReport.get("Current_NumProductIds"),
                mergeReport.get("Current_NumProductBundles"), mergeReport.get("Current_NumProductHierarchies"),
                mergeReport.get("Current_NumProductCategories"), mergeReport.get("Current_NumProductAnalytics"),
                mergeReport.get("Current_NumProductSpendings"), mergeReport.get("Current_NumObsoleteProducts")));

        return mergeReport;
    }

    public void updateMergeReport(Integer inputProductSize, Integer numInvalidProducts,
            Integer numTotalProductsAfterMerge, Map<String, Integer> productCounts) {
        mergeReport.put("Merged_NumInputProducts", inputProductSize);
        mergeReport.put("Merged_NumInvalidProducts", numInvalidProducts);
        mergeReport.put("Merged_NumProductsInTotal", numTotalProductsAfterMerge);
        mergeReport.put("Merged_NumProductIds", productCounts.get("nProductIds"));
        mergeReport.put("Merged_NumProductBundles", productCounts.get("nProductBundles"));
        mergeReport.put("Merged_NumProductHierarchies", productCounts.get("nProductHierarchies"));
        mergeReport.put("Merged_NumProductCategories", productCounts.get("nProductCategories"));
        mergeReport.put("Merged_NumProductAnalytics", productCounts.get("nProductAnalytics"));
        mergeReport.put("Merged_NumProductSpendings", productCounts.get("nProductSpendings"));
        mergeReport.put("Merged_NumObsoleteProducts", productCounts.get("nObsoleteProducts"));

        log.info(String.format(
                "%s products are consolidated. After consolidation, product table has "
                        + "%s products in total, %s invalid products, %s unique product SKUs, %s product bundles, "
                        + "%s product hierarchies, %s product categories, %s analytic products, %s spending products, "
                        + "%s obsolete products.",
                mergeReport.get("Merged_NumInputProducts"), mergeReport.get("Merged_NumProductsInTotal"),
                mergeReport.get("Merged_NumInvalidProducts"), mergeReport.get("Merged_NumProductIds"),
                mergeReport.get("Merged_NumProductBundles"), mergeReport.get("Merged_NumProductHierarchies"),
                mergeReport.get("Merged_NumProductCategories"), mergeReport.get("Merged_NumProductAnalytics"),
                mergeReport.get("Merged_NumProductSpendings"), mergeReport.get("Merged_NumObsoleteProducts")));
    }

    @VisibleForTesting
    public void setMergeReport(Map<String, Object> report) {
        this.mergeReport = report;
    }
}
