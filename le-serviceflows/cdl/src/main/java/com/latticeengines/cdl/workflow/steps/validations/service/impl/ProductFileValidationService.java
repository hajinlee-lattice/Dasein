package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.validations.service.InputFileValidationService;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.SegmentDependencyUtil;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

@Component("productFileValidationService")
@Lazy(value = false)
public class ProductFileValidationService
        extends InputFileValidationService<ProductFileValidationConfiguration> {

    public ProductFileValidationService() {
        super(ProductFileValidationConfiguration.class.getSimpleName());
    }

    private static Logger log = LoggerFactory.getLogger(ProductFileValidationService.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;


    @Override
    public long validate(ProductFileValidationConfiguration productFileValidationServiceConfiguration,
            List<String> processedRecords, StringBuilder statistics) {
        Map<String, Product> inputProducts = new HashMap<>();
        List<String> pathList = productFileValidationServiceConfiguration.getPathList();
        pathList.forEach(path -> inputProducts.putAll(loadProducts(yarnConfiguration, path, null, null)));

        Table currentTable = getCurrentConsolidateProductTable(
                productFileValidationServiceConfiguration.getCustomerSpace());
        List<Product> currentProducts = getCurrentProducts(currentTable);
        CSVFormat format = LECSVFormat.format;
        // copy error file if file exists
        String errorFile = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, errorFile, ImportProperty.ERROR_FILE);
                format = format.withSkipHeaderRecord();
            } else {
                format = format.withHeader(ImportProperty.ERROR_HEADER);
            }
        } catch (IOException e) {
            log.info("Error when copying error file to local");
        }

        // append error message to error file
        long errorLine = 0L;
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            errorLine = mergeProducts(inputProducts, currentProducts, csvFilePrinter,
                    productFileValidationServiceConfiguration.getCustomerSpace(), statistics);
        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
        }

        // copy error file back to hdfs, remove local error.csv
        if (errorLine != 0L) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, errorFile);
                }
                HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, ImportProperty.ERROR_FILE, errorFile);
                FileUtils.forceDelete(new File(ImportProperty.ERROR_FILE));
            } catch (IOException e) {
                log.info("Error when copying file to hdfs");
            }
        }
        return errorLine;
    }


    private static Map<String, Product> loadProducts(Configuration yarnConfiguration, String filePath,
            List<String> productTypes, List<String> productStatuses) {
        filePath = getPath(filePath);
        log.info("Load products from " + filePath + "/*.avro");
        Map<String, Product> productMap = new HashMap<>();

        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath + "/*.avro");
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            Product product = new Product();

            String productId = getFieldValue(record, InterfaceName.Id.name());
            if (productId == null) {
                productId = getFieldValue(record, InterfaceName.ProductId.name());
            }
            product.setProductId(productId);
            product.setProductBundle(getFieldValue(record, InterfaceName.ProductBundle.name()));
            product.setProductBundleId(getFieldValue(record, InterfaceName.ProductBundleId.name()));
            product.setProductName(getFieldValue(record, InterfaceName.ProductName.name()));
            product.setProductDescription(getFieldValue(record, InterfaceName.Description.name()));
            product.setProductLine(getFieldValue(record, InterfaceName.ProductLine.name()));
            product.setProductLineId(getFieldValue(record, InterfaceName.ProductLineId.name()));
            product.setProductFamily(getFieldValue(record, InterfaceName.ProductFamily.name()));
            product.setProductFamilyId(getFieldValue(record, InterfaceName.ProductFamilyId.name()));
            product.setProductCategory(getFieldValue(record, InterfaceName.ProductCategory.name()));
            product.setProductCategoryId(getFieldValue(record, InterfaceName.ProductCategoryId.name()));
            product.setProductType(getFieldValue(record, InterfaceName.ProductType.name()));
            product.setProductStatus(getFieldValue(record, InterfaceName.ProductStatus.name()));
            if (productTypes != null && !productTypes.contains(product.getProductType())) {
                continue;
            }
            if (productStatuses != null && !productStatuses.contains(product.getProductStatus())) {
                continue;
            }
            String lineId = getFieldValue(record, InterfaceName.InternalId.name());
            productMap.put(lineId, product);
        }
        return productMap;
    }

    private long mergeProducts(Map<String, Product> inputProducts, List<Product> currentProducts,
            CSVPrinter csvFilePrinter, CustomerSpace space, StringBuilder statistics) throws IOException {
        long errorLine = 0L;
        Map<String, Product> currentProductMap = ProductUtils.getProductMapByCompositeId(currentProducts);
        // the product map after rollup
        Map<String, Product> inputProductMap = new HashMap<>();
        boolean foundProductBundle = false;
        boolean foundProductHierarchy = false;
        for (Map.Entry<String, Product> entry : inputProducts.entrySet()) {
            Product inputProduct = entry.getValue();
            if (inputProduct.getProductId() == null) {
                continue;
            }

            if (inputProduct.getProductBundle() != null) {
                foundProductBundle = true;
                try {
                    Product analyticProduct = mergeAnalyticProduct(null, inputProduct.getProductBundle(),
                            inputProduct.getProductBundle(), inputProduct.getProductDescription(), currentProductMap,
                            inputProductMap);
                    String bundleId = analyticProduct.getProductId();
                    inputProduct.setProductBundleId(bundleId);
                    mergeBundleProduct(inputProduct, inputProductMap);
                } catch(RuntimeException e) {
                    errorLine++;
                    csvFilePrinter.printRecord(entry.getKey(), "", e.getMessage());
                }
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

                try {
                    mergeHierarchyProduct(inputProduct, inputProductMap);
                } catch (RuntimeException e) {
                    errorLine++;
                    csvFilePrinter.printRecord(entry.getKey(), "", e.getMessage());
                }
            }

            if (inputProduct.getProductBundle() == null && inputProduct.getProductCategory() == null) {
                if (inputProduct.getProductName() == null) {
                    String errMsg = "Product name is missing for product with id=" + inputProduct.getProductId();
                    errorLine++;
                    csvFilePrinter.printRecord(entry.getKey(), "", errMsg);
                }

                // ProductId will be used in avro schema in curated metrics.
                // Need to check validation
                if (!AvroUtils.isValidColumn(inputProduct.getProductId())) {
                    String errMsg = String.format("Product has invalid id=%s", inputProduct.getProductId());
                    errorLine++;
                    csvFilePrinter.printRecord(entry.getKey(), "", errMsg);
                }

                foundProductBundle = true;
                mergeAnalyticProduct(inputProduct.getProductId(), inputProduct.getProductName(),
                        inputProduct.getProductName(), inputProduct.getProductDescription(), currentProductMap,
                        inputProductMap);
            }
        }

        if (foundProductHierarchy && errorLine != 0L) {
            generateStatistics(errorLine, 0, 0, statistics);
        }
        if (foundProductBundle) {

            // get inout bundle to product list mapping
            Map<String, List<Product>> inputBundleToProductList =
                    inputProductMap.values().stream().filter(product -> ProductType.Bundle.name().equals(product.getProductType())&&
                            StringUtils.isNotBlank(product.getProductBundle())).collect(Collectors.groupingBy(Product::getProductBundle));
            Map<String, List<Product>> currentBundleToProductList =
                    currentProducts.stream().filter(product -> ProductType.Bundle.name().equals(product.getProductType())&&
                            StringUtils.isNotBlank(product.getProductBundle())).collect(Collectors.groupingBy(Product::getProductBundle));

            // input intersect the current to get attrs to be removed
            Set<String> bundleToBeRemoved =
                    currentBundleToProductList.keySet().stream().filter(bundle -> !inputBundleToProductList.containsKey(bundle)).collect(Collectors.toSet());

            List<MetadataSegment> segments = segmentProxy.getMetadataSegments(space.toString());
            Map<String, Set<String>> attrToSegName = new HashMap<>();
            // resolve attribute to segment name mapping
            if (CollectionUtils.isNotEmpty(segments)) {
                for (MetadataSegment metadataSegment : segments) {
                    SegmentDependencyUtil.findSegmentDependingAttributes(metadataSegment);
                    Set<AttributeLookup> attrLookups = metadataSegment.getSegmentAttributes();
                    for (AttributeLookup attrLookup : attrLookups) {
                        attrToSegName.putIfAbsent(attrLookup.getAttribute(), new HashSet<>());
                        attrToSegName.get(attrLookup.getAttribute()).add(metadataSegment.getDisplayName());
                    }
                }
            }

            List<RatingModel> models = ratingEngineProxy.getAllModels(space.toString());
            //resolve attribute to model name mapping
            Map<String, Set<String>> attrToModelName = new HashMap<>();
            if (CollectionUtils.isNotEmpty(models)) {
                for (RatingModel model : models) {
                    Set<AttributeLookup>  attrLookups = model.getRatingModelAttributes();
                    for (AttributeLookup attrLookup : attrLookups) {
                        // model name means rating engine name in page here
                        if (model.getRatingEngine() != null) {
                            attrToModelName.putIfAbsent(attrLookup.getAttribute(), new HashSet<>());
                            attrToModelName.get(attrLookup.getAttribute()).add(model.getRatingEngine().getDisplayName());
                        }
                    }
                }
            }
            List<RatingEngineSummary> ratingEngines = ratingEngineProxy.getRatingEngineSummaries(space.toString());
            List<RatingEngineSummary> xSellSummaries =
                    ratingEngines.stream().filter(ratingEngine -> RatingEngineType.CROSS_SELL.equals(ratingEngine.getType())).collect(Collectors.toList());

            List<RatingEngineSummary> activeXSellModel =
                    xSellSummaries.stream().filter(ratingEngine -> RatingEngineStatus.ACTIVE.equals(ratingEngine.getStatus())).collect(Collectors.toList());

            int missingBundleInUse = 0;// record num of bundle that's removed and also referenced by model or segment
            int bundleWithDiffSku = 0; // record num of bundle which have different sku

            log.info("bundle that will be removed " + JsonUtils.serialize(bundleToBeRemoved));
            // error out all bundle to be removed if existing active c-shell
            // generate warning for product list directly referenced by C-Sell model
            if (CollectionUtils.isNotEmpty(activeXSellModel)) {
                Set<String> activeXsellModelNames =
                        activeXSellModel.stream().map(RatingEngineSummary::getDisplayName).collect(Collectors.toSet());
                for (String bundle : bundleToBeRemoved) {
                    String errMsg = String.format("Error: \"%s\" can't be removed as existing active CE model %s",
                            bundle, StringUtils.join(activeXsellModelNames));
                    csvFilePrinter.printRecord("", "", errMsg);
                    errorLine++;
                }
            }
            if (CollectionUtils.isNotEmpty(xSellSummaries)) {
                // retrieve the product list in cross-sell model
                Map<String, Set<String>> bundleIdToModelName = new HashMap<>();
                Set<String> productsInUse = new HashSet<>();
                for (RatingEngineSummary summary : xSellSummaries) {
                    String engineId = summary.getId();
                    String modelId = summary.getScoringIterationId(); // scoring id is current activated model
                    if (StringUtils.isNotBlank(modelId)) {
                        RatingModel model = ratingEngineProxy.getRatingModel(space.toString(), engineId, modelId);
                        if (model instanceof AIModel) {
                            AIModel ai = (AIModel) model;
                            AdvancedModelingConfig config = ai.getAdvancedModelingConfig();
                            if (config instanceof CrossSellModelingConfig) {
                                CrossSellModelingConfig csConfig = (CrossSellModelingConfig) config;
                                //get the product list referenced by cross-sell model directly, the content here is
                                // the bundle id for product with bundle type
                                if (CollectionUtils.isNotEmpty(csConfig.getTargetProducts())) {
                                    productsInUse.addAll(csConfig.getTargetProducts());
                                }
                                if (CollectionUtils.isNotEmpty(csConfig.getTrainingProducts())) {
                                    productsInUse.addAll(csConfig.getTrainingProducts());
                                }
                                // this generate bundle id to model name mapping
                                productsInUse.forEach(product -> {
                                    bundleIdToModelName.putIfAbsent(product, new HashSet<>());
                                    bundleIdToModelName.get(product).add(summary.getDisplayName());
                                });
                            }
                        }
                    }
                }
                for (String bundle : bundleToBeRemoved) {

                    // get bundle id from the product list, compare with the bundle id directly referenced by model
                    List<Product> currentList = currentBundleToProductList.get(bundle);
                    Set<String> bundleIds =
                            currentList.stream().map(Product::getProductBundleId).collect(Collectors.toSet());
                    for (String bundleId : bundleIds) {
                        if (bundleIdToModelName.containsKey(bundleId)) {
                            String errMsg = String.format("Error: \"%s\" will be removed while also referenced by model %s",
                                    bundle, StringUtils.join(bundleIdToModelName.get(bundleId)));
                            csvFilePrinter.printRecord("", "", errMsg);
                            missingBundleInUse++;
                        }
                    }
                }
            }

            for (String bundle : bundleToBeRemoved) {
                List<Product> productList = currentBundleToProductList.get(bundle);
                for (Product product : productList) {
                    String bundleId = product.getProductBundleId();
                    // caution: bundle id is part of attribute name, get segment name or model name by fuzzy match
                    String keyForSegment =
                            attrToSegName.keySet().stream().filter(key -> key.contains(bundleId)).findFirst().orElse(null);
                    String keyForModel =
                            attrToModelName.keySet().stream().filter(key -> key.contains(bundleId)).findFirst().orElse(null);
                    Set<String> segmentNames = attrToSegName.getOrDefault(keyForSegment, new HashSet<>());
                    Set<String> modelNames = attrToModelName.getOrDefault(keyForModel, new HashSet<>());
                    // case that attr in old while not in new, also there are segment or model, error
                    if (CollectionUtils.isNotEmpty(segmentNames) || CollectionUtils.isNotEmpty(modelNames)) {
                        String segmentNameStr = CollectionUtils.isEmpty(segmentNames) ? "" :
                                StringUtils.join(segmentNames);
                        String modelNameStr = CollectionUtils.isEmpty(modelNames) ? "" : StringUtils.join(modelNames);
                        String errMsg = String.format("Error: \"%s\" which is referenced by segment %s or models %s " +
                                        "can't be removed.", bundle, segmentNameStr, modelNameStr);
                        csvFilePrinter.printRecord("", "", errMsg);
                        missingBundleInUse++;
                    }
                }
            }
            errorLine += missingBundleInUse;

            // the relationship between Bundle and sku id is one to manny,  For each Bundle in old AND in new, compare
            // the list of skus in the bundle to generate warnings
            for (Map.Entry<String, List<Product>> entry : inputBundleToProductList.entrySet()) {
                String bundle = entry.getKey();
                if (currentBundleToProductList.containsKey(bundle)) {
                    List<Product> inputList = inputBundleToProductList.get(bundle);
                    List<Product> currentList = currentBundleToProductList.get(bundle);
                    String errMsg = String.format("Warning: \"%s\" changed, Remodel may be needed for accurate scores",
                            bundle);
                    if (inputList.size() != currentList.size()) {
                        csvFilePrinter.printRecord("", "", errMsg);
                        bundleWithDiffSku++;
                        continue;
                    }
                    inputList.sort(Comparator.comparing(Product::getProductId));
                    currentList.sort(Comparator.comparing(Product::getProductId));
                    boolean change = false;
                    for (int i=0; i< inputList.size(); i++) {
                        Product pro1 = inputList.get(i);
                        Product pro2 = currentList.get(i);
                        if (!pro1.getProductId().equals(pro2.getProductId())) {
                            change = true;
                            break;
                        }
                    }
                    if (change) {
                        csvFilePrinter.printRecord("", "", errMsg);
                        bundleWithDiffSku++;
                    }
                }
            }
            // generate statistics info
            if (errorLine != 0L) {
                generateStatistics(errorLine, missingBundleInUse, bundleWithDiffSku, statistics);
            }

        }
        return errorLine;
    }

    private void generateStatistics(long errorLine, int missingBundleInUse, int bundleWithDiffSku,
                                    StringBuilder statistics) {
        statistics.append(String.format("Import failed because there were %s errors : ", String.valueOf(errorLine)));
        if (missingBundleInUse != 0) {
            statistics.append(String.format("%s missing product bundles in use (this import will " +
            "completely replace the previous one), ", String.valueOf(missingBundleInUse)));
        }
        if (bundleWithDiffSku != 0) {
            statistics.append(String.format("%s product bundle has different product SKUs. Dependant models will " +
                    "need" +
                    " to be remodelled to get accurate" +
                    " scores.", String.valueOf(bundleWithDiffSku)));
        }
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
            log.info(String.format(
                    "Create spending product [productId=%s, line=%s, family=%s, category=%s, "
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

    private Table getCurrentConsolidateProductTable(CustomerSpace customerSpace) {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        DataCollection.Version inactiveVersion = activeVersion.complement();
        Table currentTable = dataCollectionProxy.getTable(customerSpace.toString(),
                TableRoleInCollection.ConsolidatedProduct, activeVersion);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + activeVersion);
            return currentTable;
        }

        currentTable = dataCollectionProxy.getTable(customerSpace.toString(), TableRoleInCollection.ConsolidatedProduct,
                inactiveVersion);
        if (currentTable != null) {
            log.info("Found consolidated product table with version " + inactiveVersion);
            return currentTable;
        }

        log.info("There is no ConsolidatedProduct table with version " + activeVersion + " and " + inactiveVersion);
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

    private void mergeHierarchyProduct(Product inputProduct, Map<String, Product> inputProductMap) {
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
                throw new RuntimeException(errMsg);
            }

            if (StringUtils.compare(family, p.getProductFamily()) != 0) {
                String errMsg = String.format(
                        "Product with same SKU [SKU=%s] has different product families " + "[Family1=%s, Family2=%s].",
                        id, family, p.getProductFamily());
                throw new RuntimeException(errMsg);
            }

            if (StringUtils.compare(category, p.getProductCategory()) != 0) {
                String errMsg = String.format("Product with same SKU [SKU=%s] has different product categories "
                        + "[Category1=%s, Category2=%s].", id, category, p.getProductCategory());
                throw new RuntimeException(errMsg);
            }
        }

        if (StringUtils.isNotBlank(lineId)) {
            if (StringUtils.isBlank(familyId) || StringUtils.isBlank(categoryId)) {
                throw new RuntimeException(
                        String.format("Product hierarchy has level-3 but does not have level-2 or level-1. Product=%s",
                                JsonUtils.serialize(inputProduct)));
            }
        } else if (StringUtils.isNotBlank(familyId)) {
            if (StringUtils.isBlank(categoryId)) {
                throw new RuntimeException(
                        String.format("Product hierarchy has level-2 but does not have level-1. Product=%s",
                                JsonUtils.serialize(inputProduct)));
            }
        } else if (StringUtils.isBlank(categoryId)) {
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

    private Product mergeAnalyticProduct(String id, String name, String bundleName, String description,
                                         Map<String, Product> currentProductMap, Map<String, Product> inputProductMap) {
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
                throw new RuntimeException(String.format("Failed to merge analytic product. Id=%s, name=%s", id, name));
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
}
