package com.latticeengines.cdl.workflow.steps.merge;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
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

        Table inputTable = metadataProxy.getTable(
                customerSpace.toString(), TableUtils.getFullTableName(mergedBatchStoreName, pipelineVersion));
        List<Product> inputProducts = ProductUtils.loadProducts(
                yarnConfiguration, inputTable.getExtracts().get(0).getPath());

        Table currentTable = getCurrentConsolidateProductTable();
        List<Product> currentProducts = getCurrentProducts(currentTable);

        Map<String, Integer> productCounts = countProducts(currentProducts);
        mergeReport = constructMergeReport(productCounts, currentProducts.size());

        List<Product> productList = new ArrayList<>();
        Integer nInvalids = 0;
        try {
            nInvalids = mergeProducts(inputProducts, currentProducts, productList, mergeReport);
        } catch (Exception exc) {
            productList = currentProducts;
            String errMsg = "Found inconsistency during imports. Current product map will be used.";
            log.error(errMsg + exc.getMessage(), exc);
            mergeReport.put("Merged_NumProductBundles", 0);
            mergeReport.put("Merged_NumProductCategories", 0);
        }

        productCounts = countProducts(productList);
        updateMergeReport(inputProducts.size(), nInvalids, productList.size(), productCounts);
        updateEntityValueMapInContext(
                FINAL_RECORDS, (Integer) mergeReport.get("Merged_NumProductAnalytics"), Integer.class);

        try {
            ProductUtils.saveProducts(yarnConfiguration, table.getExtracts().get(0).getPath(), productList);
        } catch (Exception exc) {
            log.error("Failed to save merged products to table " + table.getName());
        }

        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        log.info(String.format("Upsert table %s to role %s, version %s.", table.getName(), TableRoleInCollection.ConsolidatedProduct, inactive));

        generateReport();
    }

    private void generateReport() {
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

        ((ObjectNode) consolidateSummaryNode).put(
                ReportConstants.PRODUCT_ID, (Integer) mergeReport.get("Merged_NumProductIds"));
        ((ObjectNode) consolidateSummaryNode).put(
                ReportConstants.PRODUCT_HIERARCHY, (Integer) mergeReport.get("Merged_NumProductCategories"));
        ((ObjectNode) consolidateSummaryNode).put(
                ReportConstants.PRODUCT_BUNDLE, (Integer) mergeReport.get("Merged_NumProductBundles"));
        ((ObjectNode) consolidateSummaryNode).put(
                ReportConstants.ERROR_MESSAGE, (String) mergeReport.get("Merged_ErrorMessage"));
        ((ObjectNode) consolidateSummaryNode).put(
                ReportConstants.WARN_MESSAGE, (String) mergeReport.get("Merged_WarnMessage"));

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
                        inputProduct.getProductBundle(), inputProduct.getProductDescription(),
                        currentProductMap, inputProductMap, mergeReport);
                String bundleId = analyticProduct.getProductId();
                inputProduct.setProductBundleId(bundleId);
                mergeBundleProduct(inputProduct, inputProductMap);
            }

            if (inputProduct.getProductCategory() != null) {
                foundProductHierarchy = true;
                String categoryId = null;
                String familyId = null;
                String lineId = null;
                Product categoryProduct = mergeSpendingProduct(null, inputProduct.getProductCategory(),
                        null, null, null, inputProduct.getProductCategory(),
                        inputProduct.getProductFamily(), inputProduct.getProductLine(),
                        currentProductMap, inputProductMap);
                categoryId = categoryProduct.getProductId();
                inputProduct.setProductCategoryId(categoryId);
                if (inputProduct.getProductFamily() != null) {
                    Product familyProduct = mergeSpendingProduct(null, inputProduct.getProductFamily(),
                            categoryId, null, null, inputProduct.getProductCategory(),
                            inputProduct.getProductFamily(), inputProduct.getProductLine(),
                            currentProductMap, inputProductMap);
                    familyId = familyProduct.getProductId();
                    inputProduct.setProductFamilyId(familyId);
                }
                if (inputProduct.getProductLine() != null) {
                    Product lineProduct = mergeSpendingProduct(null, inputProduct.getProductLine(),
                            categoryId, familyId, null, inputProduct.getProductCategory(),
                            inputProduct.getProductFamily(), inputProduct.getProductLine(),
                            currentProductMap, inputProductMap);
                    lineId = lineProduct.getProductId();
                    inputProduct.setProductLineId(lineId);
                }


                mergeHierarchyProduct(inputProduct, inputProductMap);
            }

            if (inputProduct.getProductBundle() == null && inputProduct.getProductCategory() == null) {
                if (inputProduct.getProductName() == null) {
                    if (!mergeReport.containsKey("Merged_ErrorMessage")) {
                        String errMsg = "Product name is missing for product with id=" + inputProduct.getProductId();
                        mergeReport.put("Merged_ErrorMessage", errMsg);
                    }
                    throw new RuntimeException("Invalid product name for ProductId=" + inputProduct.getProductId());
                }

                foundProductBundle = true;
                mergeAnalyticProduct(inputProduct.getProductId(), inputProduct.getProductName(),
                        inputProduct.getProductName(), inputProduct.getProductDescription(),
                        currentProductMap, inputProductMap, mergeReport);
            }
        }

        if (!foundProductBundle) {
            currentProductMap.forEach((compositeId, currentProduct) -> {
                if (currentProduct.getProductStatus().equals(ProductStatus.Active.name()) &&
                        (currentProduct.getProductType().equals(ProductType.Bundle.name()) ||
                                currentProduct.getProductType().equals(ProductType.Analytic.name()))) {
                    inputProductMap.put(compositeId, currentProduct);
                }
            });
        }

        if (!foundProductHierarchy) {
            currentProductMap.forEach((compositeId, currentProduct) -> {
                if (currentProduct.getProductStatus().equals(ProductStatus.Active.name()) &&
                        (currentProduct.getProductType().equals(ProductType.Hierarchy.name()) ||
                                currentProduct.getProductType().equals(ProductType.Spending.name()))) {
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
                                         Map<String, Product> currentProductMap,
                                         Map<String, Product> inputProductMap,
                                         Map<String, Object> mergeReport) {
        String compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name(), id, name, bundleName);
        String productId = id;

        if (id == null) {
            productId = createProductId(compositeId, currentProductMap, name);
        }

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            log.info(String.format("Found product in inputProductMap. Id=%s, compositeId=%s",
                    product.getProductId(), compositeId));

            if (!product.getProductType().equals(ProductType.Analytic.name())) {
                if (!mergeReport.containsKey("Merged_ErrorMessage")) {
                    String errMsg = String.format("Found inconsistant product type with bundle %s", bundleName);
                    mergeReport.put("Merged_ErrorMessage", errMsg);
                }
                throw new RuntimeException(String.format("Failed to merge analytic product. Id=%s, name=%s", id, name));
            }

            if (!product.getProductDescription().equals(description) &&
                    !mergeReport.containsKey("Merged_WarnMessage")) {
                String warnMsg = String.format("Found inconsistent product description with bundle %s.", bundleName);
                mergeReport.put("Merged_WarnMessage", warnMsg);
            }
        } else {
            log.info("No product in inputProductMap. A new analytic product will be created.");
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

    private Product mergeBundleProduct(Product inputProduct, Map<String, Product> inputProductMap) {
        String id = inputProduct.getProductId();
        String name = inputProduct.getProductName();
        String description = inputProduct.getProductDescription();
        String bundle = inputProduct.getProductBundle();
        String compositeId = ProductUtils.getCompositeId(ProductType.Bundle.name(), id, name, bundle);

        log.info("A new bundle product will be created.");
        Product newProduct = new Product();
        newProduct.setProductId(id);
        newProduct.setProductName(name);
        newProduct.setProductDescription(description);
        newProduct.setProductBundle(bundle);
        newProduct.setProductBundleId(inputProduct.getProductBundleId());
        newProduct.setProductType(ProductType.Bundle.name());
        inputProductMap.put(compositeId, newProduct);

        return newProduct;
    }

    private Product mergeSpendingProduct(String id, String name, String categoryId, String familyId, String lineId,
                                         String category, String family, String line,
                                         Map<String, Product> currentProductMap, Map<String, Product> inputProductMap) {
        String compositeId = ProductUtils.getCompositeId(ProductType.Spending.name(), id, name, null);
        String productId = id;

        if (id == null) {
            productId = createProductId(compositeId, currentProductMap, name);
        }

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            log.info(String.format("Found product in inputProductMap. Id=%s, compositeId=%s", product.getProductId(), compositeId));
            boolean categoryError = false, familyError = false, lineError = false;
            if (product.getProductCategoryId() != null && !product.getProductCategoryId().equals(categoryId)) {
                categoryError = true;
            }
            if (product.getProductFamilyId() != null && !product.getProductFamilyId().equals(familyId)) {
                familyError = true;
            }
            if (product.getProductLineId() != null && !product.getProductLineId().equals(lineId)) {
                lineError = true;
            }
            if (categoryError || familyError || lineError) {
                throw new RuntimeException(String.format("Failed to merge spending product. Id=%s, name=%s", id, name));
            }
        } else {
            log.info("No product in inputProductMap. A new spending product will be created.");
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

    private Product mergeHierarchyProduct(Product inputProduct, Map<String, Product> inputProductMap) {
        String id = inputProduct.getProductId();
        String name = inputProduct.getProductName();
        String description = inputProduct.getProductDescription();
        String bundle = inputProduct.getProductBundle();
        String lineId = inputProduct.getProductLineId();
        String familyId = inputProduct.getProductFamilyId();
        String categoryId = inputProduct.getProductCategoryId();
        String compositeId = ProductUtils.getCompositeId(ProductType.Hierarchy.name(), id, name, bundle);

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            throw new RuntimeException(String.format("Hierarchy product already exists. InputProduct: %s",
                    JsonUtils.serialize(inputProduct)));
        } else {
            if (StringUtils.isNotBlank(lineId)) {
                if (StringUtils.isBlank(familyId) || StringUtils.isBlank(categoryId)) {
                    if (!mergeReport.containsKey("Merged_ErrorMessage")) {
                        String errMsg = String.format(
                                "Found invalid product hierarchy with id = %s, name = %s", id, name);
                        mergeReport.put("Merged_ErrorMessage", errMsg);
                    }

                    throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                            JsonUtils.serialize(inputProduct)));
                }
            } else if (StringUtils.isNotBlank(familyId)) {
                if (StringUtils.isBlank(categoryId)) {
                    if (!mergeReport.containsKey("Merged_ErrorMessage")) {
                        String errMsg = String.format(
                                "Found invalid product hierarchy with id = %s, name = %s", id, name);
                        mergeReport.put("Merged_ErrorMessage", errMsg);
                    }

                    throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                            JsonUtils.serialize(inputProduct)));
                }
            } else if (StringUtils.isBlank(categoryId)) {
                if (!mergeReport.containsKey("Merged_ErrorMessage")) {
                    String errMsg = String.format(
                            "Found invalid product hierarchy with id = %s, name = %s", id, name);
                    mergeReport.put("Merged_ErrorMessage", errMsg);
                }

                throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                        JsonUtils.serialize(inputProduct)));
            }

            log.info("A new hierarchy product will be created.");
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

            return newProduct;
        }
    }

    public Map<String, Integer> countProducts(List<Product> products) {
        Set<String> productIdSet = new HashSet<>();
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
        result.put("nProductCategories", productCategorySet.size());
        result.put("nProductAnalytics", productAnalyticSet.size());
        result.put("nProductSpendings", productSpendingSet.size());
        result.put("nObsoleteProducts", obsoleteProducts.size());
        return result;
    }

    private String createProductId(String compositeId, Map<String, Product> currentProductMap, String productName) {
        Product currentProduct = currentProductMap.get(compositeId);
        if (currentProduct != null) {
            log.info(String.format("Found product with compositeId=%s, productId=%s in currentProductMap.",
                    compositeId, currentProduct.getProductId()));
            return currentProduct.getProductId();
        } else {
            log.info(String.format("No product with compositeId=%s found in currentProductMap. " +
                    "Generating hashed productId based on productName=%s.", compositeId, productName));
            return HashUtils.getCleanedString(HashUtils.getShortHash(productName));
        }
    }

    private Table createMergedConsolidatedProductTable() {
        Table table = SchemaRepository.instance().getSchema(SchemaInterpretation.Product, true);
        String fullTableName = TableUtils.getFullTableName(batchStore.name(), pipelineVersion);
        table.setName(fullTableName);
        table.setDisplayName(fullTableName);
        String hdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "").toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath + "/" + fullTableName + "/" + pipelineVersion);
            log.info(String.format("Initialized merged product table %s/%s/%s", hdfsPath, fullTableName, pipelineVersion));
        } catch (Exception exc) {
            log.error(String.format("Failed to initialize merged product table %s/%s/%s", hdfsPath, fullTableName, pipelineVersion));
            throw new RuntimeException("Failed to create merged product table.");
        }
        Extract extract = new Extract();
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setName("extract_target");
        extract.setProcessedRecords(1L);
        extract.setPath(hdfsPath + "/" + fullTableName + "/" + pipelineVersion + "/*.avro");
        table.setExtracts(Collections.singletonList(extract));
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);

        return table;
    }

    private Table getCurrentConsolidateProductTable() {
        Table currentTable = dataCollectionProxy.getTable(
                customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct, active);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + active);
            return currentTable;
        }

        currentTable = dataCollectionProxy.getTable(
                customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct, inactive);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + inactive);
            return currentTable;
        }

        log.info("There is no ConsoidatedProduct table with version " + active + " and " + inactive);
        return null;
    }

    private List<Product> getCurrentProducts(Table currentConsolidateProductTable) {
        List<Product> currentProducts;
        if (currentConsolidateProductTable != null) {
            currentProducts = ProductUtils.loadProducts(yarnConfiguration,
                    currentConsolidateProductTable.getExtracts().get(0).getPath());
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

    public Map<String, Object> constructMergeReport(Map<String, Integer> productCounts,
                                                    Integer currentProductsSize) {
        Map<String, Object> mergeReport = new HashMap<>();
        mergeReport.put("Current_NumProductsInTotal", currentProductsSize);
        mergeReport.put("Current_NumProductIds", productCounts.get("nProductIds"));
        mergeReport.put("Current_NumProductBundles", productCounts.get("nProductBundles"));
        mergeReport.put("Current_NumProductCategories", productCounts.get("nProductCategories"));
        mergeReport.put("Current_NumProductAnalytics", productCounts.get("nProductAnalytics"));
        mergeReport.put("Current_NumProductSpendings", productCounts.get("nProductSpendings"));
        mergeReport.put("Current_NumObsoleteProducts", productCounts.get("nObsoleteProducts"));

        log.info(String.format("In current product list, there are %s products in total, %s unique product SKUs, " +
                        "%s product bundles, %s product categories, %s analytic products, %s spending products, " +
                        "%s obsolete products.",
                mergeReport.get("Current_NumProductsInTotal"), mergeReport.get("Current_NumProductIds"),
                mergeReport.get("Current_NumProductBundles"), mergeReport.get("Current_NumProductCategories"),
                mergeReport.get("Current_NumProductAnalytics"), mergeReport.get("Current_NumProductSpendings"),
                mergeReport.get("Current_NumObsoleteProducts")));

        return mergeReport;
    }

    public void updateMergeReport(Integer inputProductSize, Integer numInvalidProducts,
                                  Integer numTotalProductsAfterMerge, Map<String, Integer> productCounts) {
        mergeReport.put("Merged_NumInputProducts", inputProductSize);
        mergeReport.put("Merged_NumInvalidProducts", numInvalidProducts);
        mergeReport.put("Merged_NumProductsInTotal", numTotalProductsAfterMerge);
        mergeReport.put("Merged_NumProductIds", productCounts.get("nProductIds"));
        mergeReport.put("Merged_NumProductBundles", productCounts.get("nProductBundles"));
        mergeReport.put("Merged_NumProductCategories", productCounts.get("nProductCategories"));
        mergeReport.put("Merged_NumProductAnalytics", productCounts.get("nProductAnalytics"));
        mergeReport.put("Merged_NumProductSpendings", productCounts.get("nProductSpendings"));
        mergeReport.put("Merged_NumObsoleteProducts", productCounts.get("nObsoleteProducts"));
    }

    @VisibleForTesting
    public void setMergeReport(Map<String, Object> report) {
        this.mergeReport = report;
    }
}
