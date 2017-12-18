package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.util.AwsApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.core.BaseAwsPythonBatchStep;

@Component("awsApsGeneratorStep")
public class AwsApsGeneratorStep extends BaseAwsPythonBatchStep<AWSPythonBatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(AwsApsGeneratorStep.class);
    private static final String APS = TableRoleInCollection.AnalyticPurchaseState.name();

    @Value("${cdl.aps.generate.enabled}")
    private boolean apsEnabled;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
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
        Table periodTable = dataCollectionProxy.getTable(config.getCustomerSpace().toString(),
                TableRoleInCollection.AggregatedPeriodTransaction, inactive);
        if (!apsEnabled || periodTable == null) {
            log.warn("Aps generation is disabled or there's not metadata table for period aggregated table!");
            return;
        }
        List<String> inputPaths = getInputPaths(periodTable);
        config.setInputPaths(inputPaths);
        String hdfsPath = getOutputPath(config);
        config.setOutputPath(hdfsPath);

    }

    @Override
    protected void afterComplete(AWSPythonBatchConfiguration config) {
        try {
            if (AvroUtils.count(yarnConfiguration, newHdfsPath + "/*.avro") > 0) {
                String customerSpace = configuration.getCustomerSpace().toString();
                Table apsTable = MetaDataTableUtils.createTable(yarnConfiguration, newTableName, newHdfsPath);
                apsTable.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
                AwsApsGeneratorUtils.setupMetaData(apsTable);
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
}
