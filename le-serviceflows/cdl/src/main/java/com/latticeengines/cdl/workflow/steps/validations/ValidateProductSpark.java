package com.latticeengines.cdl.workflow.steps.validations;

import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ProductValidationSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.ProductFileConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ValidateProductConfig;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.domain.exposed.util.SegmentDependencyUtil;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.cdl.ValidateProduct;

@Component("ValidateProductSpark")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ValidateProductSpark extends RunSparkJob<ProductFileConfiguration, ValidateProductConfig> {

    private static final Logger log = LoggerFactory.getLogger(ValidateProductSpark.class);

    private static final String PATH_SEPARATOR = "/";

    private static final String MESSAGE = "Message";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    private static final String S3_ATLAS_DATA_TABLE_DIR = "/%s/atlas/Data/Tables";

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String bucket;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    private List<String> productPathList;

    private Table currentProductTable;

    @Override
    protected Class<ValidateProduct> getJobClz() {
        return ValidateProduct.class;
    }

    @Override
    protected ValidateProductConfig configureJob(ProductFileConfiguration configuration) {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        String tenantId = configuration.getCustomerSpace().getTenantId();

        if (applicationId == null) {
            log.warn("There's no application Id! tenantId=" + tenantId);
            return null;
        }
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for applicationId=%s, tenantId=%s", applicationId,
                    tenantId));
            return null;
        }
        List<String> pathList = eaiImportJobDetail.getPathDetail();
        pathList = pathList == null ? null : pathList.stream().filter(StringUtils::isNotBlank).map(path -> {
            int index = path.indexOf("/Pods/");
            path = index > 0 ? path.substring(index) : path;
            return path;
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(pathList)) {
            log.warn(String.format("Avro path is empty for applicationId=%s, tenantId=%s", applicationId, tenantId));
            return null;
        }

        productPathList = pathList;

        HdfsDataUnit newInput = getNewProducts(productPathList);
        HdfsDataUnit oldInput = getOldProducts();
        List<DataUnit> inputList = new ArrayList<>();
        inputList.add(newInput);
        if (oldInput != null) {
            inputList.add(oldInput);
        }
        ValidateProductConfig validateConfig = new ValidateProductConfig();
        validateConfig.setCheckProductName(checkExistProductNameInTemplate(configuration.getCustomerSpace(),
                configuration.getDataFeedTaskId()));

        validateConfig.setInput(inputList);
        validateConfig.setWorkspace(getRandomWorkspace());
        return validateConfig;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        HdfsDataUnit newAnalytic = result.getTargets().get(0);
        HdfsDataUnit errs = result.getTargets().get(1);

        ProductValidationSummary summary = JsonUtils.deserialize(result.getOutput(), ProductValidationSummary.class);
        long errorLine = summary.getErrorLineNumber();
        // copy error file if file exists
        String errorFile = ProductUtils.getPath(productPathList.get(0)) + PATH_SEPARATOR + ImportProperty.ERROR_FILE;
        CSVFormat format = copyErrorFileToLocalIfExist(errorFile);
        try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ImportProperty.ERROR_FILE, true), format)) {
            if (errs.getCount() != 0) {
                String errorPath = errs.getPath();
                writeErrorCSVFromAvro(errorPath, csvFilePrinter);
            }
            // found bundle records
            if (newAnalytic.getCount() != 0L) {
                String inputPath =  newAnalytic.getPath();
                List<Product> inputProducts = loadAnalyticProducts(yarnConfiguration, inputPath);
                Map<String, List<Product>> inputBundleToProductList = inputProducts
                        .stream()
                        .collect(Collectors.groupingBy(Product::getProductBundle));
                String currentPath = currentProductTable == null ? null :
                        currentProductTable.getExtracts().get(0).getPath();
                List<Product> currentProducts = loadAnalyticProducts(yarnConfiguration, currentPath);
                Map<String, List<Product>> currentBundleToProductList = currentProducts
                        .stream()
                        .collect(Collectors.groupingBy(Product::getProductBundle));
                // bundle validation logic
                Set<String> inputBundles = inputBundleToProductList.keySet();
                Set<String> currentBundles = currentBundleToProductList.keySet();
                Set<String> newAddedBundles = inputBundles
                        .stream()
                        .filter(name -> !currentBundles.contains(name))
                        .collect(toSet());
                Set<String> bundleToBeRemoved = currentBundles
                        .stream()
                        .filter(name -> !inputBundles.contains(name))
                        .collect(toSet());
                summary.setAddedBundles(newAddedBundles);
                summary.setMissingBundles(bundleToBeRemoved);
                summary.setProcessedBundles(currentBundles);
                errorLine += checkBundleDependence(inputBundleToProductList, currentBundleToProductList,
                        bundleToBeRemoved, csvFilePrinter, summary);
            }
        } catch (Exception e) {
            log.info("Error when writing error message to error file");
        }
        summary.setErrorLineNumber(errorLine);
        if (errorLine != 0L) {
            copyErrorFileBackToHdfs(errorFile, configuration.getCustomerSpace().getTenantId(),
                    productPathList.get(0));
        }
        putObjectInContext(ENTITY_VALIDATION_SUMMARY, summary);
    }

    private void writeErrorCSVFromAvro(String errorPath, CSVPrinter csvFilePrinter) throws Exception {
        List<String> avroFileList = HdfsUtils.getFilesByGlob(yarnConfiguration, errorPath + "/*.avro");
        for (String avroFile : avroFileList) {
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration,
                    new Path(avroFile))) {
                for (GenericRecord record : reader) {
                    csvFilePrinter.printRecord("", "", getFieldValue(record, MESSAGE));
                }
            }
        }

    }

    private boolean checkExistProductNameInTemplate(CustomerSpace space, String dataFeedTaskId) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(space.toString(), dataFeedTaskId);
        if (dataFeedTask == null) {
            log.info(String.format("dataFeedTask is null for space %s and task id %s.", space.toString(), dataFeedTaskId));
            return false;
        }
        Table table = dataFeedTask.getImportTemplate();
        if (table == null) {
            log.info(String.format("template table is null for task id %s.", dataFeedTaskId));
            return false;
        }
        Attribute attr = table.getAttribute(InterfaceName.ProductName.name());
        return attr != null;

    }

    private long checkBundleDependence(Map<String, List<Product>> inputBundleToProductList,
                                 Map<String, List<Product>> currentBundleToProductList,
                                 Set<String> bundleToBeRemoved, CSVPrinter csvFilePrinter,
                                 ProductValidationSummary productValidationSummary) throws Exception {

        Pair<Integer, Boolean> statsForCE = checkCEModelCase(bundleToBeRemoved, currentBundleToProductList,
                csvFilePrinter);

        int missingBundleInModelOrSegment = checkDependantSegmentOrModel(bundleToBeRemoved, currentBundleToProductList,
                csvFilePrinter);

        int missingBundleInUse = statsForCE.getLeft() + missingBundleInModelOrSegment;

        int bundleWithDiffSku = checkSKUInBundle(inputBundleToProductList, currentBundleToProductList, csvFilePrinter);
        productValidationSummary.setDifferentSKU(bundleWithDiffSku);
        productValidationSummary.setMissingBundleInUse(missingBundleInUse);
        return Boolean.TRUE.equals(statsForCE.getRight()) ? missingBundleInUse + 1 : missingBundleInUse;
    }


    private Pair<Integer, Boolean> checkCEModelCase(Set<String> bundleToBeRemoved,
                                                    Map<String, List<Product>> currentBundleToProductList,
                                                    CSVPrinter csvFilePrinter) throws Exception {
        int missingBundleInUse = 0;
        boolean errorActiveCE = false;
        List<RatingEngineSummary> ratingEngines = ratingEngineProxy
                .getRatingEngineSummaries(configuration.getCustomerSpace().toString());
        List<RatingEngineSummary> xSellSummaries = ratingEngines
                .stream()
                .filter(ratingEngine -> RatingEngineType.CROSS_SELL.equals(ratingEngine.getType()))
                .collect(Collectors.toList());

        List<RatingEngineSummary> activeXSellModel = xSellSummaries
                .stream()
                .filter(ratingEngine -> RatingEngineStatus.ACTIVE.equals(ratingEngine.getStatus()))
                .collect(Collectors.toList());
        log.info("bundle that will be removed " + JsonUtils.serialize(bundleToBeRemoved));
        // error out all bundle to be removed if existing active c-shell
        // generate warning for product list directly referenced by C-Sell model
        if (CollectionUtils.isNotEmpty(activeXSellModel)) {
            Set<String> activeXsellModelNames = activeXSellModel
                    .stream()
                    .map(RatingEngineSummary::getDisplayName)
                    .collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(bundleToBeRemoved)) {
                String errMsg = String.format("Error: \"%s\" can't be removed as existing active cross sell model" +
                                " %s",
                        StringUtils.join(bundleToBeRemoved), StringUtils.join(activeXsellModelNames));
                csvFilePrinter.printRecord("", "", errMsg);
                errorActiveCE = true;
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
                    RatingModel model = ratingEngineProxy
                            .getRatingModel(configuration.getCustomerSpace().toString(), engineId, modelId);
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
                Set<String> bundleIds = currentList
                        .stream()
                        .map(Product::getProductBundleId)
                        .collect(Collectors.toSet());
                for (String bundleId : bundleIds) {
                    if (bundleIdToModelName.containsKey(bundleId)) {
                        String errMsg = String.format("Error: \"%s\" which is directly referenced by " +
                                        "model %s can't be removed.",
                                bundle, StringUtils.join(bundleIdToModelName.get(bundleId)));
                        csvFilePrinter.printRecord("", "", errMsg);
                        missingBundleInUse++;
                        // only print one error for one-to-many relationship of bundle to product id
                        break;
                    }
                }
            }
        }
        return Pair.of(missingBundleInUse, errorActiveCE);
    }

    private int checkDependantSegmentOrModel(Set<String> bundleToBeRemoved,
                                             Map<String, List<Product>> currentBundleToProductList,
                                             CSVPrinter csvFilePrinter) throws Exception {
        int missingBundleInUse = 0;
        List<MetadataSegment> segments = segmentProxy.getMetadataSegments(configuration.getCustomerSpace().toString());
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

        List<RatingModel> models = ratingEngineProxy.getAllModels(configuration.getCustomerSpace().toString());
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
                    break;
                }
            }
        }
        return missingBundleInUse;
    }

    private int checkSKUInBundle(Map<String, List<Product>> inputBundleToProductList,
                         Map<String, List<Product>> currentBundleToProductList,
                         CSVPrinter csvFilePrinter) throws Exception {
        // the relationship between Bundle and sku id is one to manny,  For each Bundle in old AND in new, compare
        // the list of skus in the bundle to generate warnings
        int bundleWithDiffSku = 0;
        for (Map.Entry<String, List<Product>> entry : inputBundleToProductList.entrySet()) {
            String bundle = entry.getKey();
            if (currentBundleToProductList.containsKey(bundle)) {
                List<Product> inputList = inputBundleToProductList.get(bundle);
                List<Product> currentList = currentBundleToProductList.get(bundle);
                String errMsg = String.format("Warning: \"%s\" changed, Remodel may be needed for accurate scores",
                        bundle);
                Set<String> inputIdSet = inputList.stream().map(Product::getProductId).collect(Collectors.toSet());
                Set<String> currentIdSet = currentList.stream().map(Product::getProductId).collect(Collectors.toSet());
                if (!inputIdSet.equals(currentIdSet)) {
                    csvFilePrinter.printRecord("", "", errMsg);
                    bundleWithDiffSku++;
                }
            }
        }
        return bundleWithDiffSku;
    }

    private HdfsDataUnit getNewProducts(List<String> pathList) {
        String[] avroPaths = new String[pathList.size()];
        avroPaths = pathList.toArray(avroPaths);
        String tabName = NamingUtils.uuid("productValidation");
        Table inputTable = MetaDataTableUtils.createTable(yarnConfiguration, tabName, avroPaths, null);
        return inputTable.toHdfsDataUnit("new");
    }

    private HdfsDataUnit getOldProducts() {
        currentProductTable = getCurrentConsolidateProductTable(configuration.getCustomerSpace());
        if (currentProductTable != null) {
            log.info("Found consolidated product table" );
            return currentProductTable.toHdfsDataUnit("old");
        } else {
            log.info("There is no ConsolidatedProduct table");
            return null;
        }
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

    private CSVFormat copyErrorFileToLocalIfExist(String errorFile) {
        CSVFormat format = LECSVFormat.format;
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
        return format;
    }

    private static List<Product> loadAnalyticProducts(Configuration yarnConfiguration, String filePath) {
        List<Product> productList = new ArrayList<>();
        if (StringUtils.isBlank(filePath)) {
            return productList;
        }
        filePath = ProductUtils.getPath(filePath);
        log.info("Load products from " + filePath + "/*.avro");

        Iterator<GenericRecord> iter = AvroUtils.iterateAvroFiles(yarnConfiguration, filePath + "/*.avro");
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            String productType = getFieldValue(record, InterfaceName.ProductType.name());
            if (!ProductType.Analytic.name().equals(productType)) {
                continue;
            }
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
            productList.add(product);
        }
        return productList;
    }

    private static String getFieldValue(GenericRecord record, String field) {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = null;
        }
        return value;
    }

    protected void copyErrorFileBackToHdfs(String errorFile, String... params) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                HdfsUtils.rmdir(yarnConfiguration, errorFile);
            }
            if (params != null && params.length == 2) {
                copyErrorFileToS3(params[0], params[1]);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, ImportProperty.ERROR_FILE, errorFile);
            FileUtils.forceDelete(new File(ImportProperty.ERROR_FILE));
        } catch (IOException e) {
            log.info("Error when copying file to hdfs");
        }
    }

    protected void copyErrorFileToS3(String tenantId, String path) {
        StringBuilder sb = new StringBuilder();
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();
        String hdfsTablesDir = pathBuilder.getHdfsAtlasTablesDir(podId, tenantId);
        String key = sb.append(String.format(S3_ATLAS_DATA_TABLE_DIR, tenantId))
                .append(ProductUtils.getPath(path).substring(hdfsTablesDir.length()))
                .append(PATH_SEPARATOR)
                .append(ImportProperty.ERROR_FILE)
                .toString();
        s3Service.uploadLocalFile(bucket, key, new File(ImportProperty.ERROR_FILE), true);
    }
}
