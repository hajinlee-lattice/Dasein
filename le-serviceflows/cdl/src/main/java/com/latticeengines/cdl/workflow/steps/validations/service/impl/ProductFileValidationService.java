package com.latticeengines.cdl.workflow.steps.validations.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

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

    @Override
    public long validate(ProductFileValidationConfiguration productFileValidationServiceConfiguration,
            List<String> processedRecords) {
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
            errorLine = mergeProducts(inputProducts, currentProducts, csvFilePrinter);
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
            CSVPrinter csvFilePrinter) throws IOException {
        long errorLine = 0L;
        Map<String, Product> currentProductMap = ProductUtils.getProductMapByCompositeId(currentProducts);
        Map<String, Product> inputProductMap = new HashMap<>();
        for (Map.Entry<String, Product> entry : inputProducts.entrySet()) {
            Product inputProduct = entry.getValue();
            if (inputProduct.getProductId() == null) {
                continue;
            }
            if (inputProduct.getProductBundle() == null && inputProduct.getProductCategory() == null
                    && inputProduct.getProductName() == null) {
                errorLine++;
                csvFilePrinter.printRecord(entry.getKey(), "",
                        String.format(
                                "Product name, category, bundle can't be all empty for product with productId =%s",
                                inputProduct.getProductId()));
            }
            if (inputProduct.getProductCategory() != null) {
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
        }
        return errorLine;
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

}
