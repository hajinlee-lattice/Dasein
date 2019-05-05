package com.latticeengines.cdl.workflow.steps.validations;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.BaseInputFileValidatorConfiguration;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseInputFileValidator<T extends BaseInputFileValidatorConfiguration>
        extends BaseWorkflowStep<T> {
    private static final Logger log = LoggerFactory.getLogger(BaseInputFileValidator.class);

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    private static final List<Character> invalidChars = Arrays.asList('/', '&');

    protected abstract BusinessEntity getEntity();

    @Override
    public void execute() {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        String tenantId = configuration.getCustomerSpace().getTenantId();

        if (applicationId == null) {
            log.warn("There's no application Id! tenentId=" + tenantId);
            return;
        }
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for applicationId=%s, tenantId=%s", applicationId,
                    tenantId));
            return;
        }
        List<String> pathList = eaiImportJobDetail.getPathDetail();
        pathList = pathList == null ? null
                : pathList.stream().filter(StringUtils::isNotBlank).map(path -> {
            int index = path.indexOf("/Pods/");
            path = index > 0 ? path.substring(index) : path;
            return path;
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pathList)) {
            log.warn(String.format("Avro path is empty for applicationId=%s, tenantId=%s", applicationId, tenantId));
            return;
        }

        BusinessEntity entity = getEntity();
        log.info(String.format("Begin to validate data with entity %s.", entity.name()));
        if (BusinessEntity.Account == entity) {
            validateAccount(pathList);
        } else if (BusinessEntity.Contact == entity) {
            validateContact(pathList);
        } else if (BusinessEntity.Product == entity) {
            validateProduct(pathList);
        }
    }

    private void validateAccount(List<String> pathList) {
        List<String> errorMessages = new ArrayList<>();
        try (AvroFilesIterator iterator = AvroUtils.avroFileIterator(yarnConfiguration, pathList)) {
            while (iterator.hasNext()) {
                GenericRecord record = iterator.next();
                String id = getString(record, InterfaceName.Id.name());
                if (id == null) {
                    id = getString(record, InterfaceName.AccountId.name());
                }
                if (StringUtils.isEmpty(id)) {
                    log.info("Empty id is found from avro file");
                    continue;
                }
                for (Character c : invalidChars) {
                    if (id.indexOf(c) != -1) {
                        errorMessages
                                .add(String.format("Invalid account id is found due to %s in %s.", c.toString(), id));
                        break;
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(errorMessages)) {
            String filePath = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
            writeErrorFile(filePath, errorMessages);
            throw new LedpException(LedpCode.LEDP_40059, new String[] { ImportProperty.ERROR_FILE });
        }
    }

    private void validateContact(List<String> pathList) {
        List<String> errorMessages = new ArrayList<>();
        try (AvroFilesIterator iterator = AvroUtils.avroFileIterator(yarnConfiguration, pathList)) {
            while (iterator.hasNext()) {
                GenericRecord record = iterator.next();
                String id = getString(record, InterfaceName.Id.name());
                if (StringUtils.isBlank(id)) {
                    id = getString(record, InterfaceName.ContactId.name());
                }
                if (StringUtils.isNotBlank(id)) {
                    continue;
                }
                String email = getString(record, InterfaceName.Email.name());
                if (StringUtils.isNotBlank(email)) {
                    continue;
                }
                String firstName = getString(record, InterfaceName.FirstName.name());
                String lastName = getString(record, InterfaceName.LastName.name());
                String phone = getString(record, InterfaceName.PhoneNumber.name());
                if (StringUtils.isNotBlank(firstName) && StringUtils.isNotBlank(lastName)
                        && StringUtils.isNotBlank(phone)) {
                    continue;
                }
                errorMessages.add(
                        "The contact does not have sufficient information. The contact should have should have at least one of the three mentioned: 1. Contact ID  2. Email 3. First name + last name + phone");
            }
        }
        if (CollectionUtils.isNotEmpty(errorMessages)) {
            String filePath = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
            writeErrorFile(filePath, errorMessages);
            throw new LedpException(LedpCode.LEDP_40059, new String[] { ImportProperty.ERROR_FILE });
        }
    }

    private void validateProduct(List<String> pathList) {
        List<Product> inputProducts = new ArrayList<>();
        pathList.forEach(path -> inputProducts.addAll(ProductUtils.loadProducts(yarnConfiguration, path, null, null)));
        Table currentTable = getCurrentConsolidateProductTable(configuration.getCustomerSpace());
        List<Product> currentProducts = getCurrentProducts(currentTable);
        List<String> errorMessages = new ArrayList<>();
        mergeProducts(inputProducts, currentProducts, errorMessages);
        if (CollectionUtils.isNotEmpty(errorMessages)) {
            String filePath = getPath(pathList.get(0)) + "/" + ImportProperty.ERROR_FILE;
            writeErrorFile(filePath, errorMessages);
            throw new LedpException(LedpCode.LEDP_40059, new String[] { ImportProperty.ERROR_FILE });
        }
    }

    private void writeErrorFile(String filePath, List<String> errorMessages) {

        CSVFormat format = LECSVFormat.format;
        // copy error file if file exists
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, filePath)) {
                HdfsUtils.copyHdfsToLocal(yarnConfiguration, filePath, ImportProperty.ERROR_FILE);
            } else {
                format = format.withHeader(ImportProperty.ERROR_HEADER);
            }
        } catch (IOException e) {
            log.info("Error when copying file to local");
        }
        // append error message to error file
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            for (String message : errorMessages) {
                csvFilePrinter.printRecord("", "", message);
            }
            csvFilePrinter.close();
        } catch (IOException ex) {
            log.info("Error when writing error message to error file");
        }
        // copy error file back to hdfs
        try {
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, ImportProperty.ERROR_FILE, filePath);
        } catch (IOException e) {
            log.info("Error when copying file to hdfs");
        }
    }

    private List<Product> mergeProducts(List<Product> inputProducts, List<Product> currentProducts,
            List<String> errorMessages) {
        Map<String, Product> currentProductMap = ProductUtils.getProductMapByCompositeId(currentProducts);
        Map<String, Product> inputProductMap = new HashMap<>();
        for (Product inputProduct : inputProducts) {
            if (inputProduct.getProductId() == null) {
                continue;
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
                    errorMessages.add(e.getMessage());
                }
            }
        }
        return new ArrayList<>(inputProductMap.values());
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

    private static String getPath(String avroDir) {
        log.info("Get avro path input " + avroDir);
        if (!avroDir.endsWith(".avro")) {
            return avroDir;
        } else {
            String[] dirs = avroDir.trim().split("/");
            avroDir = "";
            for (int i = 0; i < (dirs.length - 1); i++) {
                log.info("Get avro path dir " + dirs[i]);
                if (!dirs[i].isEmpty()) {
                    avroDir = avroDir + "/" + dirs[i];
                }
            }
        }
        log.info("Get avro path output " + avroDir);
        return avroDir;
    }

    private static String getString(GenericRecord record, String field) {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = null;
        }
        return value;
    }
}
