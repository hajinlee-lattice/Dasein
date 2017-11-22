package com.latticeengines.cdl.workflow.steps;

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
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.core.BaseAwsPythonBatchStep;

@Component("awsApsGeneratorStep")
public class AwsApsGeneratorStep extends BaseAwsPythonBatchStep<AWSPythonBatchConfiguration> {

    private static final String APS = "AnalyticPurchaseState";

    @Value("${cdl.aps.generate.enabled:false}")
    private boolean apsEnabled;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private static final Logger log = LoggerFactory.getLogger(AwsApsGeneratorStep.class);

    @Override
    protected String getAcondaEnv() {
        return "v01";
    }

    @Override
    protected String getPythonScript() {
        return "apsgenerator.py";
    }

    @Override
    protected void setupConfig(AWSPythonBatchConfiguration config) {
        Table periodTable = dataCollectionProxy.getTable(config.getCustomerSpace().toString(),
                TableRoleInCollection.ConsolidatedPeriodTransaction);
        if (periodTable == null) {
            log.warn("There's not metadata table for period aggregated table!");
            return;
        }
        /*
         * config.setInputPaths(Arrays.asList("/Pods/Aps/input/*.avro"));
         * config.setOutputPath("/Pods/Aps/output");
         */
        List<String> inputPaths = getInputPaths(periodTable);
        config.setInputPaths(inputPaths);
        String hdfsPath = getOutputPath(config);
        config.setOutputPath(hdfsPath);

    }

    @Override
    protected void afterComplete(AWSPythonBatchConfiguration config) {
        try {
            String hdfsPath = PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "").append(APS)
                    .toString();
            String newHdfsPath = PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "")
                    .append(APS + "_New").toString();
            String bakHdfsPath = PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "")
                    .append(APS + "_Bak").toString();
            if (HdfsUtils.fileExists(yarnConfiguration, newHdfsPath)) {
                if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                    if (HdfsUtils.fileExists(yarnConfiguration, bakHdfsPath)) {
                        HdfsUtils.rmdir(yarnConfiguration, bakHdfsPath);
                    }
                    HdfsUtils.rename(yarnConfiguration, hdfsPath, bakHdfsPath);
                    HdfsUtils.rename(yarnConfiguration, newHdfsPath, hdfsPath);
                } else {
                    HdfsUtils.rename(yarnConfiguration, newHdfsPath, hdfsPath);
                }

                Table apsTable = MetaDataTableUtils.createTable(yarnConfiguration, APS, hdfsPath);
                apsTable.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
                metadataProxy.updateTable(configuration.getCustomerSpace().toString(), APS, apsTable);
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
            String newHdfsPath = PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "")
                    .append(APS + "_New").toString();
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
