package com.latticeengines.cdl.workflow.steps.process;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ApsGenerationStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.ApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.PeriodStrategyUtils;
import com.latticeengines.domain.exposed.util.ProductUtils;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkScript;

@Component("apsGeneration")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ApsGeneration extends RunSparkScript<ApsGenerationStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ApsGeneration.class);
    private static final String APS = TableRoleInCollection.AnalyticPurchaseState.name();

    @Inject
    private ManifestService manifestService;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Value("${cdl.aps.generation.partition.multiplier}")
    private int apsPartitionMultiplier;

    private DataCollection.Version active;
    private DataCollection.Version inactive;
    private Table periodTable;
    private Map<String, List<Product>> productMap;

    @Override
    protected String getScriptPath() {
        return "hdfs://" + manifestService.getLedsPath() + File.separator + "spark/scripts/ApsGeneration.scala";
    }

    @Override
    protected void preScriptExecution() {
        active = dataCollectionProxy.getActiveVersion(configuration.getCustomer());
        inactive = active.complement();
        List<Table> periodTables = getPeriodTables(configuration);
        if (periodTables == null) {
            log.warn("Aps generation is disabled or there's not metadata table for period aggregated table!");
            skipScriptExecution = true;
            return;
        }
        String rollingPeriod = configuration.getRollingPeriod();
        if (StringUtils.isBlank(rollingPeriod)) {
            throw new RuntimeException("Must specify rolling period in configuration.");
        }
        log.info("Rolling Period=" + rollingPeriod);
        periodTable = PeriodStrategyUtils.findPeriodTableFromStrategyName(periodTables, rollingPeriod);
        log.info("Period Transaction Table=" + periodTable.getName());

        productMap = loadProductMap();
        if (MapUtils.isEmpty(productMap)) {
            throw new RuntimeException("There's either no product table or no Analytic products");
        }

        setPartitionMultiplier(apsPartitionMultiplier);
    }

    @Override
    protected JsonNode getParams() {
        Map<String, String> params = new HashMap<>();
        params.put("AccountIdKey", "AccountId");
        params.put("PeriodIdKey", "PeriodId");
        params.put("ProductIdKey", "ProductId");
        params.put("AmountKey", "TotalAmount");
        params.put("QuantityKey", "TotalQuantity");
        params.put("ProductTypeKey", "ProductType");
        params.put("ApsImputationEnabledKey", getConfiguration().isApsImputationEnabled() + "");
        return JsonUtils.convertValue(params, JsonNode.class);
    }

    protected void postScriptExecution(SparkJobResult result) {
        String apsTableName = NamingUtils.timestamp(APS);
        Table apsTable = toTable(apsTableName, InterfaceName.AnalyticPurchaseState_ID.name(), //
                result.getTargets().get(0));
        ApsGeneratorUtils.setupMetaData(apsTable, productMap);
        metadataProxy.updateTable(CustomerSpace.parse(configuration.getCustomer()).toString(), apsTableName, apsTable);
        dataCollectionProxy.upsertTable(configuration.getCustomer(), apsTableName,
                TableRoleInCollection.AnalyticPurchaseState, inactive);
    }

    @Override
    protected List<DataUnit> getInputUnits() {
        HdfsDataUnit txnInput = periodTable.toHdfsDataUnit("Transaction");
        return Collections.singletonList(txnInput);
    }

    private List<Table> getPeriodTables(ApsGenerationStepConfiguration config) {
        List<Table> periodTables = dataCollectionProxy.getTables(config.getCustomer(),
                TableRoleInCollection.ConsolidatedPeriodTransaction, inactive);
        if (CollectionUtils.isEmpty(periodTables)) {
            periodTables = dataCollectionProxy.getTables(config.getCustomer(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction, active);
            if (CollectionUtils.isNotEmpty(periodTables)) {
                log.info("Found period stores in active version " + active);
            }
        } else {
            log.info("Found period stores in inactive version " + inactive);
        }
        return periodTables;
    }

    private Map<String, List<Product>> loadProductMap() {
        Table productTable = dataCollectionProxy.getTable(configuration.getCustomer(),
                TableRoleInCollection.ConsolidatedProduct, inactive);
        if (productTable == null) {
            log.info("Did not find product table in inactive version");
            productTable = dataCollectionProxy.getTable(configuration.getCustomer(),
                    TableRoleInCollection.ConsolidatedProduct, active);
            if (productTable == null) {
                log.warn("Cannot find the product table in both versions");
                return null;
            }
        }
        log.info(String.format("productTableName for customer %s is %s", configuration.getCustomer(),
                productTable.getName()));
        List<Product> productList = new ArrayList<>(ProductUtils.loadProducts(yarnConfiguration, //
                productTable.getExtracts().get(0).getPath(), //
                Collections.singletonList(ProductType.Analytic.name()), null));
        return ProductUtils.getProductMap(productList);
    }
}
