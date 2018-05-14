package com.latticeengines.cdl.workflow.steps.process;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.service.ZKComponentService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.util.AwsApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.core.BaseAwsPythonBatchStep;

@Component("awsApsGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AwsApsGeneratorStep extends BaseAwsPythonBatchStep<AWSPythonBatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AwsApsGeneratorStep.class);
    private static final String APS = TableRoleInCollection.AnalyticPurchaseState.name();

    @Value("${cdl.aps.generate.enabled}")
    private boolean apsEnabled;

    @Value("${cdl.aps.generate.in.aws}")
    private boolean apsInAws;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private ZKComponentService zkComponentService;

    @Inject
    private Configuration yarnConfiguration;

    private DataCollection.Version active;
    private DataCollection.Version inactive;

    private String newTableName;
    private String newHdfsPath;

    @Override
    protected String getCondaEnv() {
        return "v01";
    }

    @Override
    protected String getPythonScript() {
        return "apsgenerator.py";
    }

    @Override
    protected void setupConfig(AWSPythonBatchConfiguration config) {
        active = dataCollectionProxy.getActiveVersion(config.getCustomerSpace().toString());
        inactive = active.complement();
        List<Table> periodTables = getPeriodTables(config);
        if (!apsEnabled || periodTables == null) {
            log.warn("Aps generation is disabled or there's not metadata table for period aggregated table!");
            return;
        }
        PeriodStrategy rollingPeriod = zkComponentService.getRollingPeriod(config.getCustomerSpace());
        log.info("Rolling Period=" + rollingPeriod.getName());
        Table periodTable = PeriodStrategyUtils.findPeriodTableFromStrategy(periodTables, rollingPeriod);
        config.setRunInAws(apsInAws);
        List<String> inputPaths = getInputPaths(periodTable);
        config.setInputPaths(inputPaths);
        String hdfsPath = getOutputPath(config);
        config.setOutputPath(hdfsPath);
    }

    private List<Table> getPeriodTables(AWSPythonBatchConfiguration config) {
        List<Table> periodTables = dataCollectionProxy.getTables(config.getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        if (CollectionUtils.isEmpty(periodTables)) {
            periodTables = dataCollectionProxy.getTables(config.getCustomerSpace().toString(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction, active);
            if (CollectionUtils.isNotEmpty(periodTables)) {
                log.info("Found period stores in active version " + active);
            }
        } else {
            log.info("Found period stores in inactive version " + inactive);
        }
        return periodTables;
    }

    @Override
    protected void afterComplete(AWSPythonBatchConfiguration config) {
        try {
            if (AvroUtils.count(yarnConfiguration, newHdfsPath + "/*.avro") > 0) {
                String customerSpace = configuration.getCustomerSpace().toString();
                Table apsTable = MetaDataTableUtils.createTable(yarnConfiguration, newTableName, newHdfsPath);
                apsTable.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
                Map<String, List<Product>> productMap = loadProductMap(config);
                AwsApsGeneratorUtils.setupMetaData(apsTable, productMap);
                metadataProxy.updateTable(customerSpace, newTableName, apsTable);
                dataCollectionProxy.upsertTable(customerSpace, newTableName,
                        TableRoleInCollection.AnalyticPurchaseState, inactive);
            } else {
                throw new RuntimeException("There's no new APS file created!");
            }

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<String, List<Product>> loadProductMap(AWSPythonBatchConfiguration config) {
        Table productTable = dataCollectionProxy.getTable(config.getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version.");
            productTable = dataCollectionProxy.getTable(config.getCustomerSpace().toString(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                log.warn("Cannot find the product table in both versions");
                return null;
            }
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                productTable.getName()));
        List<Product> productList = new ArrayList<>(
                ProductUtils.loadProducts(yarnConfiguration, productTable.getExtracts().get(0).getPath()));
        Map<String, List<Product>> productMap = ProductUtils.getProductMap(productList, ProductType.Analytic.name());
        return productMap;
    }

    private List<String> getInputPaths(Table transactionTable) {
        List<String> inputPaths = new ArrayList<>();
        for (Extract extract : transactionTable.getExtracts()) {
            if (!extract.getPath().endsWith("*.avro")) {
                inputPaths.add(extract.getPath() + "/*.avro");
            } else {
                inputPaths.add(extract.getPath());
            }
        }
        return inputPaths;
    }

    private String getOutputPath(AWSPythonBatchConfiguration config) {
        try {
            newTableName = NamingUtils.timestamp(APS);
            newHdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "")
                    .append(newTableName).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, newHdfsPath)) {
                HdfsUtils.rmdir(yarnConfiguration, newHdfsPath);
            }
            HdfsUtils.mkdir(yarnConfiguration, newHdfsPath);
            return newHdfsPath;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void localizePythonScripts() {
        try {
            String scriptDir = getScriptDirInHdfs();
            InputStream is = HdfsUtils.getInputStream(yarnConfiguration, scriptDir + "/leframework.tar.gz");
            CompressionUtils.untarInputStream(is, getPythonWorkspace().getPath());
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/pythonlauncher.sh",
                    getPythonWorkspace().getPath() + "/pythonlauncher.sh");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/apsdataloader.py",
                    getPythonWorkspace().getPath() + "/apsdataloader.py");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, scriptDir + "/apsgenerator.py",
                    getPythonWorkspace().getPath() + "/apsgenerator.py");
        } catch (IOException e) {
            throw new RuntimeException("Failed to localize python scripts", e);
        }
    }

}
