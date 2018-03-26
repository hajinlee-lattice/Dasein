package com.latticeengines.cdl.workflow.steps.merge;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

@Component(MergeProduct.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProduct extends BaseSingleEntityMergeImports<ProcessProductStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MergeProduct.class);

    static final String BEAN_NAME = "mergeProduct";
    private Map<String, Integer> mergeReport;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeProduct");
            TransformationStepConfig merge = mergeInputs(true, true, false);
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

        Table inputTable = metadataProxy.getTable(customerSpace.toString(), TableUtils.getFullTableName(mergedBatchStoreName, pipelineVersion));
        List<Product> inputProducts = ProductUtils.loadProducts(yarnConfiguration, inputTable.getExtracts().get(0).getPath());

        List<Product> currentProducts;
        Table currentTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct, active);
        if (currentTable != null) {
            currentProducts = ProductUtils.loadProducts(yarnConfiguration, currentTable.getExtracts().get(0).getPath());
        } else {
            currentProducts = new ArrayList<>();
        }

        mergeReport = new HashMap<>();
        Map<String, Integer> productCounts = countProducts(currentProducts);
        mergeReport.put("Current_NumBundleProducts", productCounts.get("nBundle"));
        mergeReport.put("Current_NumHierarchyProducts", productCounts.get("nHierarchy"));
        mergeReport.put("Current_NumAnalyticProducts", productCounts.get("nAnalytic"));
        mergeReport.put("Current_NumSpendingProducts", productCounts.get("nSpending"));

        List<Product> productList;
        try {
            productList = mergeProducts(inputProducts, currentProducts, mergeReport);
        } catch (Exception exc) {
            productList = currentProducts;
            log.error("Found inconsistency during imports. Current product map will be used.");
        }

        try {
            ProductUtils.saveProducts(yarnConfiguration, table.getExtracts().get(0).getPath(), productList);
        } catch (Exception exc) {
            log.error("Failed to save merged products to table " + table.getName());
        }

        dataCollectionProxy.upsertTable(customerSpace.toString(), table.getName(),
                TableRoleInCollection.ConsolidatedProduct, active);
        log.info(String.format("Upsert table %s to role %s, version %s.", table.getName(), TableRoleInCollection.ConsolidatedProduct, active));

//        generateReport(mergeReport);
    }

    public void generateReport(Map<String, Integer> mergedReport1) {
        ObjectNode report = getObjectFromContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(),
                    ObjectNode.class);
//        ObjectNode report = JsonUtils.deserialize("{\n" +
//                "    \"SystemActions\": [\n" +
//                "        \"Rebuild due to Data Cloud Version Changed\"\n" +
//                "    ],\n" +
//                "    \"EntitiesSummary\": {\n" +
//                "        \"Account\": {\n" +
//                "            \"ConsolidateRecordsSummary\": {\n" +
//                "                \"NEW\": \"500\",\n" +
//                "                \"UPDATE\": \"0\",\n" +
//                "                \"UNMATCH\": \"11\",\n" +
//                "                \"DELETE\": \"0\"\n" +
//                "            },\n" +
//                "            \"EntityStatsSummary\": {\n" +
//                "                \"TOTAL\": \"500\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"Contact\": {\n" +
//                "            \"ConsolidateRecordsSummary\": {\n" +
//                "                \"NEW\": \"1100\",\n" +
//                "                \"UPDATE\": \"0\",\n" +
//                "                \"DELETE\": \"0\"\n" +
//                "            },\n" +
//                "            \"EntityStatsSummary\": {\n" +
//                "                \"TOTAL\": \"1100\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"Product\": {\n" +
//                "            \"ConsolidateRecordsSummary\": {},\n" +
//                "            \"EntityStatsSummary\": {\n" +
//                "                \"TOTAL\": \"100\"\n" +
//                "            }\n" +
//                "        },\n" +
//                "        \"Transaction\": {\n" +
//                "            \"ConsolidateRecordsSummary\": {\n" +
//                "                \"NEW\": \"0\",\n" +
//                "                \"DELETE\": \"0\"\n" +
//                "            },\n" +
//                "            \"EntityStatsSummary\": {\n" +
//                "                \"TOTAL\": \"0\"\n" +
//                "            }\n" +
//                "        }\n" +
//                "    }\n" +
//                "}", ObjectNode.class);

        ObjectMapper om = JsonUtils.getObjectMapper();
        ObjectNode newItem;
        try {
            newItem = (ObjectNode) om.readTree(JsonUtils.serialize(mergedReport1));
        } catch (IOException exc) {
            log.error("Failed to generate report for merging products. " + exc);
            throw new RuntimeException("Failed to generate report for merging products. " + exc);
        }
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
        ((ObjectNode) consolidateSummaryNode).setAll((ObjectNode) newItem.get(entity.name()));


        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), report);
    }

    public List<Product> mergeProducts(List<Product> inputProducts, List<Product> currentProducts,
                                       Map<String, Integer> report) {
        int nInputs = inputProducts.size();
        int nInvalids = 0;
        boolean foundProductBundle = false;
        boolean foundProductHierarchy = false;
        Map<String, Product> inputProductMap = new HashMap<>();
        Map<String, Product> currentProductMap = ProductUtils.getProductMapByCompositeId(currentProducts);

        for (Product inputProduct : inputProducts) {
            if (inputProduct.getProductId() == null) {
                nInvalids ++;
                continue;
            }

            if (inputProduct.getProductBundle() != null) {
                foundProductBundle = true;
                Product analyticProduct = mergeAnalyticProduct(null, inputProduct.getProductBundle(),
                        inputProduct.getProductBundle(), currentProductMap, inputProductMap);
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
                foundProductBundle = true;
                mergeAnalyticProduct(inputProduct.getProductId(), inputProduct.getProductName(),
                        inputProduct.getProductName(), currentProductMap, inputProductMap);
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

        Map<String, Integer> productCounts = countProducts(new ArrayList<>(inputProductMap.values()));
        report.put("Merged_NumTotalInputs", nInputs);
        report.put("Merged_NumInvalids", nInvalids);
        report.put("Merged_NumBundleProducts", productCounts.get("nBundle"));
        report.put("Merged_NumHierarchyProducts", productCounts.get("nHierarchy"));
        report.put("Merged_NumAnalyticProducts", productCounts.get("nAnalytic"));
        report.put("Merged_NumSpendingProducts", productCounts.get("nSpending"));

        return new ArrayList<>(inputProductMap.values());
    }

    private Product mergeAnalyticProduct(String id, String name, String bundleName,
                                         Map<String, Product> currentProductMap,
                                         Map<String, Product> inputProductMap) {
        String compositeId = ProductUtils.getCompositeId(ProductType.Analytic.name(), id, name, bundleName);
        String productId = id;

        if (id == null) {
            productId = createProductId(compositeId, currentProductMap);
        }

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
            if (!product.getProductType().equals(ProductType.Analytic.name())) {
                throw new RuntimeException(String.format("Failed to merge analytic product. Id=%s, name=%s", id, name));
            }
        } else {
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
        String compositeId = ProductUtils.getCompositeId(inputProduct.getProductType(), id, name, bundle);

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
            productId = createProductId(compositeId, currentProductMap);
        }

        Product product = inputProductMap.get(compositeId);
        if (product != null) {
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
                    throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                            JsonUtils.serialize(inputProduct)));
                }
            } else if (StringUtils.isNotBlank(familyId)) {
                if (StringUtils.isBlank(categoryId)) {
                    throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                            JsonUtils.serialize(inputProduct)));
                }
            } else if (StringUtils.isBlank(categoryId)) {
                throw new RuntimeException(String.format("Failed to validate hierarchy for product %s",
                        JsonUtils.serialize(inputProduct)));
            }

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

    private Map<String, Integer> countProducts(List<Product> products) {
        Map<String, Integer> result = new HashMap<>();
        int nBundle = 0, nHierarchy = 0, nAnalytic = 0, nSpending = 0;

        for (Product product : products) {
            if (product.getProductStatus().equals(ProductStatus.Obsolete.name())) {
                continue;
            }

            switch (ProductType.valueOf(product.getProductType())) {
                case Bundle:
                    nBundle ++;
                    break;
                case Analytic:
                    nAnalytic ++;
                    break;
                case Hierarchy:
                    nHierarchy ++;
                    break;
                case Spending:
                    nSpending ++;
                    break;
                case Raw:
                default:
                    break;
            }
        }

        result.put("nBundle", nBundle);
        result.put("nHierarchy", nHierarchy);
        result.put("nAnalytic", nAnalytic);
        result.put("nSpending", nSpending);
        return result;
    }

    private String createProductId(String compositeId, Map<String, Product> currentProductMap) {
        Product currentProduct = currentProductMap.get(compositeId);
        return (currentProduct != null) ? currentProduct.getProductId() : UUID.randomUUID().toString();
    }
}
