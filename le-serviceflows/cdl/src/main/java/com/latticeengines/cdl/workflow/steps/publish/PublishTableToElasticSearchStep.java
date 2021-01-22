package com.latticeengines.cdl.workflow.steps.publish;

import static com.latticeengines.cdl.workflow.steps.publish.PublishTableToElasticSearchStep.BEAN_NAME;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountLookup;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.TimelineProfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish.PublishTableToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.PublishTableToElasticSearchJobConfiguration;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.job.cdl.PublishTableToElasticSearchJob;

@Component(BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class PublishTableToElasticSearchStep extends RunSparkJob<PublishTableToElasticSearchStepConfiguration,
        PublishTableToElasticSearchJobConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PublishTableToElasticSearchStep.class);

    static final String BEAN_NAME = "publishTableToElasticSearchStep";

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ElasticSearchService elasticSearchService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    private List<ElasticSearchExportConfig> configs;



    @Override
    protected Class<? extends AbstractSparkJob<PublishTableToElasticSearchJobConfiguration>> getJobClz() {
        return PublishTableToElasticSearchJob.class;
    }

    @Override
    protected PublishTableToElasticSearchJobConfiguration configureJob(PublishTableToElasticSearchStepConfiguration stepConfiguration) {
        configs = stepConfiguration.getExportConfigs();
        if (CollectionUtils.isEmpty(configs)) {
            log.info("empty export configs for {}", customerSpace);
            return null;
        }
        String customizedSignature = stepConfiguration.getSignature();

        List<DataUnit> units = new ArrayList<>();
        Map<Integer, TableRoleInCollection> indexToRole = new HashMap<>();
        Map<Integer, String> indexToSignature = new HashMap<>();
        Integer number = 0;
        for (ElasticSearchExportConfig config : configs) {
            if (StringUtils.isNotBlank(customizedSignature)) {
                log.info("using signature {} instead for {}", customizedSignature, customerSpace);
                config.setSignature(customizedSignature);
            }

            if (StringUtils.isBlank(config.getTableName()) ||
                    StringUtils.isBlank(config.getSignature()) ||
                    config.getTableRoleInCollection() == null) {
                log.info("table name, signature, table role must be provided, skip processing this config {} in " +
                                "customer {}", JsonUtils.serialize(config), customerSpace);
                continue;
            }
            TableRoleInCollection role = config.getTableRoleInCollection();
            if (!createElasticSearchIndex(config, configuration.getEsConfigs())) {
                log.info("failed to create index or update mapping for {}", customerSpace);
                continue;
            }

            String tableName = config.getTableName();
            String version = config.getSignature();
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            DataUnit unit = table.toHdfsDataUnit(tableName);
            units.add(unit);
            indexToRole.put(number, role);
            indexToSignature.put(number, version);
            number++;
        }

        PublishTableToElasticSearchJobConfiguration config = new PublishTableToElasticSearchJobConfiguration();
        config.setInput(units);
        config.setIndexToRole(indexToRole);
        config.setIndexToSignature(indexToSignature);
        config.setEsConfigs(configuration.getEsConfigs());
        config.setCustomerSpace(customerSpace.toString());
        return config;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        // register data unit, currently one tenant has 3 data unit
        // identical with index
        for (ElasticSearchExportConfig config : configs) {
            TableRoleInCollection role = config.getTableRoleInCollection();
            if (StringUtils.isBlank(config.getTableName()) ||
                    StringUtils.isBlank(config.getSignature()) ||
                    config.getTableRoleInCollection() == null) {
                log.info("table name, signature, table role must be provided, skip processing this config {} in " +
                        "customer {}", JsonUtils.serialize(config), customerSpace);
                continue;
            }
            String tableName = ElasticSearchUtils.getEntityFromTableRole(role);
            ElasticSearchDataUnit unit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(), tableName,
                    DataUnit.StorageType.ElasticSearch);
            if (unit == null || !config.getSignature().equals(unit.getSignature())) {
                log.info("elastic search data unit will be updated to {}", config.getSignature());
                ElasticSearchDataUnit dataUnit = new ElasticSearchDataUnit();
                dataUnit.setName(tableName);
                dataUnit.setSignature(config.getSignature());
                dataUnitProxy.create(customerSpace.toString(), dataUnit);
            }
        }

    }

    private boolean createElasticSearchIndex(ElasticSearchExportConfig exportConfig, ElasticSearchConfig esConfig) {

        String signature = exportConfig.getSignature();
        TableRoleInCollection role = exportConfig.getTableRoleInCollection();
        String entity = ElasticSearchUtils.getEntityFromTableRole(role);
        if (StringUtils.isBlank(entity)) {
            log.info("entity is empty for role {} in {}", role, customerSpace);
            return false;
        }

        String idxName = ElasticSearchUtils.constructIndexName(customerSpace.toString(),
                entity, signature);
        log.info("create index {} with entity {} and version {}", idxName, entity, signature);
        // create or update index according to role
        if (BusinessEntity.Account.name().equals(entity)) {
            // TODO get columns from account look up
            elasticSearchService.createAccountIndexWithLookupIds(idxName, esConfig,
                    Collections.singletonList(InterfaceName.AccountId.name()));
        } else {
            elasticSearchService.createIndexWithSettings(idxName, esConfig, entity);
        }

        log.info("update mapping for index {} with new column {} if needed", idxName, role);
        if (TimelineProfile.name().equals(role.name())) {
            log.info("no column for timeline profile");
        } else if (AccountLookup.name().equals(role.name())) {
            log.info("set nested to fieldName {}", role.name());
            elasticSearchService.updateIndexMapping(idxName, role.name(), "nested");
        } else {
            log.info("set binary to fieldName {}", role.name());
            elasticSearchService.updateIndexMapping(idxName, role.name(), "binary");
        }
        return true;
    }

}
