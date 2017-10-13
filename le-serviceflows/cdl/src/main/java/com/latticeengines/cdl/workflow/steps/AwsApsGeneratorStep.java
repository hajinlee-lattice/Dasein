package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.workflow.core.BaseAwsPythonBatchStep;

@Component("awsApsGeneratorStep")
public class AwsApsGeneratorStep extends BaseAwsPythonBatchStep<AWSPythonBatchConfiguration> {

    @Value("${consolidate.aps.generate.enabled:false}")
    private boolean apsEnabled;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @SuppressWarnings("unused")
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
        Table aggrTable = getObjectFromContext(ConsolidateTransactionData.AGGREGATE_TABLE_KEY, Table.class);
        if (!apsEnabled || aggrTable == null) {
            log.info("There's no need for AggregatedTransaction");
            return;
        }
        Table transactionTable = dataCollectionProxy.getTable(config.getCustomerSpace().toString(),
                TableRoleInCollection.AggregatedTransaction);
        if (transactionTable == null) {
            log.warn("There's not metadata table for AggregatedTransaction");
            return;
        }
        /*
         * config.setInputPaths(Arrays.asList("/Pods/Aps/input/*.avro"));
         * config.setOutputPath("/Pods/Aps/output");
         */
        List<String> inputPaths = getInputPaths(transactionTable);
        config.setInputPaths(inputPaths);
        String hdfsPath = getOutputPath(config);
        config.setOutputPath(hdfsPath);

    }

    private List<String> getInputPaths(Table transactionTable) {
        List<String> inputPaths = new ArrayList<>();
        for (Extract extract : transactionTable.getExtracts()) {
            if (!extract.getPath().endsWith("*.avro")) {
                inputPaths.add(extract.getPath());
            } else {
                inputPaths.add(extract.getPath() + "/*.avro");
            }
        }
        return inputPaths;
    }

    private String getOutputPath(AWSPythonBatchConfiguration config) {
        try {
            String hdfsPath = PathBuilder
                    .buildDataTablePath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), "")
                    .append("AnalyticPurchaseState").toString();
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
                HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
            }
            return hdfsPath;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
