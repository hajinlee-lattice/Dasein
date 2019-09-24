package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroStreamsIterator;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProductUtils {
    private static final Logger log = LoggerFactory.getLogger(ProductUtils.class);
    private static String FILE_NAME = "Products.avro";

    public static String getCompositeId(String type, String id, String name, String bundle, String category,
            String family, String line) {
        if (StringUtils.isBlank(type)) {
            return null;
        } else {
            try {
                switch (ProductType.valueOf(type)) {
                case Bundle:
                    return StringUtils.join(new String[] { type, id, bundle }, "__");
                case Hierarchy:
                    return StringUtils.join(new String[] { type, id }, "__");
                case Analytic:
                    if (StringUtils.isNotEmpty(id)) {
                        return StringUtils.join(new String[] { type, id }, "__");
                    } else {
                        return StringUtils.join(new String[] { type, name }, "__");
                    }
                case Spending:
                    return StringUtils.join(new String[] { type, name, category, family, line }, "__");
                case Raw:
                default:
                    return null;
                }
            } catch (IllegalArgumentException exc) {
                log.error(String.format("Type %s is unknown for ProductType enum.", type));
                return null;
            }
        }
    }

    public static List<Product> loadProducts(Configuration yarnConfiguration, String filePath,
            List<String> productTypes, List<String> productStatuses) {
        filePath = getPath(filePath);
        log.info("Load products from " + filePath + "/*.avro");

        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath + "/*.avro");
        return loadProducts(iter, productTypes == null ? null : new HashSet<>(productTypes),
                productStatuses == null ? null : new HashSet<>(productStatuses));
    }

    public static List<Product> loadProducts(Iterator<InputStream> streamIter, List<String> productTypes,
            List<String> productStatuses) {
        try (AvroStreamsIterator iter = AvroUtils.iterateAvroStreams(streamIter)) {
            List<Product> products = loadProducts(iter, productTypes == null ? null : new HashSet<>(productTypes),
                    productStatuses == null ? null : new HashSet<>(productStatuses));
            return products;
        }
    }

    public static List<Product> loadProducts(Iterator<GenericRecord> iter, Set<String> productTypes,
            Set<String> productStatuses) {
        List<Product> productList = new ArrayList<>();

        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            Product product = new Product();

            String productId = getString(record, InterfaceName.Id.name());
            if (productId == null) {
                productId = getString(record, InterfaceName.ProductId.name());
            }
            product.setProductId(productId);
            product.setProductBundle(getString(record, InterfaceName.ProductBundle.name()));
            product.setProductBundleId(getString(record, InterfaceName.ProductBundleId.name()));
            product.setProductName(getString(record, InterfaceName.ProductName.name()));
            product.setProductDescription(getString(record, InterfaceName.Description.name()));
            product.setProductLine(getString(record, InterfaceName.ProductLine.name()));
            product.setProductLineId(getString(record, InterfaceName.ProductLineId.name()));
            product.setProductFamily(getString(record, InterfaceName.ProductFamily.name()));
            product.setProductFamilyId(getString(record, InterfaceName.ProductFamilyId.name()));
            product.setProductCategory(getString(record, InterfaceName.ProductCategory.name()));
            product.setProductCategoryId(getString(record, InterfaceName.ProductCategoryId.name()));
            product.setProductType(getString(record, InterfaceName.ProductType.name()));
            product.setProductStatus(getString(record, InterfaceName.ProductStatus.name()));
            if (productTypes != null && !productTypes.contains(product.getProductType())) {
                continue;
            }
            if (productStatuses != null && !productStatuses.contains(product.getProductStatus())) {
                continue;
            }
            productList.add(product);
        }
        return productList;
    }

    public static long countProducts(Configuration yarnConfiguration, String filePath,
            List<String> productTypes, String... productStatuses) {
        Set<String> productIds = new HashSet<>();
        filePath = getPath(filePath);
        Set<String> statuses = null;
        if (productStatuses != null && productStatuses.length > 0) {
            statuses = new HashSet<>(Arrays.asList(productStatuses));
        }
        log.info("Load products from " + filePath + "/*.avro");
        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath + "/*.avro");
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String productId = getString(record, InterfaceName.Id.name());
            if (productId == null) {
                productId = getString(record, InterfaceName.ProductId.name());
            }
            String productType = getString(record, InterfaceName.ProductType.name());
            if (productTypes != null && !productTypes.contains(productType)) {
                continue;
            }
            String productStatus = getString(record, InterfaceName.ProductStatus.name());
            if (CollectionUtils.isNotEmpty(statuses) && !statuses.contains(productStatus)) {
                continue;
            }
            productIds.add(productId);
        }
        return productIds.size();
    }

    public static void saveProducts(Configuration yarnConfiguration, String filePath, List<Product> productList)
            throws IOException {
        filePath = getPath(filePath);
        log.info("Save products to " + filePath + "/" + FILE_NAME);
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        SchemaRepository.instance().getSchema(BusinessEntity.Product).getAttributes().forEach(attribute -> {
            columns.add(Pair.of(attribute.getName(), String.class));
        });
        columns.add(Pair.of(InterfaceName.Id.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductBundleId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductLineId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductFamilyId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductCategoryId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductType.name(), String.class));
        columns.add(Pair.of(InterfaceName.ProductStatus.name(), String.class));
        Schema schema = AvroUtils.constructSchema(BusinessEntity.Product.name(), columns);

        List<GenericRecord> data = new ArrayList<>();
        for (Product product : productList) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            for (Schema.Field field : schema.getFields()) {
                switch (InterfaceName.valueOf(field.name())) {
                case Id:
                case ProductId:
                    builder.set(field, product.getProductId());
                    if (field.name().equals(InterfaceName.ProductId.name())) {
                        builder.set(InterfaceName.Id.name(), product.getProductId());
                    } else {
                        builder.set(InterfaceName.ProductId.name(), product.getProductId());
                    }
                    break;
                case ProductName:
                    builder.set(field, product.getProductName());
                    break;
                case Description:
                    builder.set(field, product.getProductDescription());
                    break;
                case ProductBundle:
                    builder.set(field, product.getProductBundle());
                    break;
                case ProductLine:
                    builder.set(field, product.getProductLine());
                    break;
                case ProductFamily:
                    builder.set(field, product.getProductFamily());
                    break;
                case ProductCategory:
                    builder.set(field, product.getProductCategory());
                    break;
                case ProductType:
                    builder.set(field, product.getProductType());
                    break;
                case ProductStatus:
                    builder.set(field, product.getProductStatus());
                    break;
                case ProductBundleId:
                    builder.set(field, product.getProductBundleId());
                    break;
                case ProductLineId:
                    builder.set(field, product.getProductLineId());
                    break;
                case ProductFamilyId:
                    builder.set(field, product.getProductFamilyId());
                    break;
                case ProductCategoryId:
                    builder.set(field, product.getProductCategoryId());
                    break;
                default:
                    log.warn(String.format("Found unknown field %s when saving product.", field.name()));
                    break;
                }
            }
            data.add(builder.build());
        }

        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, filePath + "/" + FILE_NAME, data, true);
    }

    public static Map<String, List<Product>> getProductMap(List<Product> productList, String... productTypes) {
        Map<String, List<Product>> productMap = new HashMap<>();
        productList = filterProductListByType(productList, productTypes);

        productList.forEach(product -> {
            if (productMap.get(product.getProductId()) != null) {
                productMap.get(product.getProductId()).add(product);
            } else {
                List<Product> products = new ArrayList<>();
                products.add(product);
                productMap.put(product.getProductId(), products);
            }
        });
        return productMap;
    }

    public static Map<String, List<Product>> getActiveProductMap(List<Product> productList, String... productTypes) {
        Map<String, List<Product>> productMap = new HashMap<>();
        productList = filterProductListByType(productList, productTypes);
        productList = filterProductListByStatus(productList, ProductStatus.Active.name());

        productList.forEach(product -> {
            if (productMap.get(product.getProductId()) != null) {
                productMap.get(product.getProductId()).add(product);
            } else {
                List<Product> products = new ArrayList<>();
                products.add(product);
                productMap.put(product.getProductId(), products);
            }
        });

        return productMap;
    }

    public static Map<String, Product> getProductMapByCompositeId(List<Product> productList, String... statuses) {
        Map<String, Product> productMap = new HashMap<>();
        productList = filterProductListByStatus(productList, statuses);

        productList.forEach(product -> {
            String compositeId = getCompositeId(product.getProductType(), product.getProductId(),
                    product.getProductName(), product.getProductBundle(), product.getProductCategory(),
                    product.getProductFamily(), product.getProductLine());
            productMap.put(compositeId, product);
        });

        return productMap;
    }

    public static boolean hasAnalyticProduct(Configuration yarnConfiguration, Table productTable) {
        return hasProductType(yarnConfiguration, productTable, ProductType.Analytic.name());
    }

    public static boolean hasProductType(Configuration yarnConfiguration, Table productTable, String productType) {
        boolean foundProductType = false;
        String filePath = getPath(productTable.getExtracts().get(0).getPath());
        log.info("Load products from " + filePath + "/*.avro");
        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath + "/*.avro");
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String typeInFile = getString(record, InterfaceName.ProductType.name());
            if (productType.equals(typeInFile)) {
                foundProductType = true;
                break;
            }
        }
        return foundProductType;
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

    private static List<Product> filterProductListByType(List<Product> productList, String... productTypes) {
        if (productTypes != null && productTypes.length > 0) {
            Set<String> typeSet = new HashSet<>(Arrays.asList(productTypes));
            productList = productList.stream()
                    .filter(product -> product.getProductType() == null || typeSet.contains(product.getProductType()))
                    .collect(Collectors.toList());
        }

        return productList;
    }

    private static List<Product> filterProductListByStatus(List<Product> productList, String... productStatuses) {
        if (productStatuses != null && productStatuses.length > 0) {
            Set<String> statusSet = new HashSet<>(Arrays.asList(productStatuses));
            productList = productList.stream().filter(
                    product -> product.getProductStatus() == null || statusSet.contains(product.getProductStatus()))
                    .collect(Collectors.toList());
        }

        return productList;
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
}
