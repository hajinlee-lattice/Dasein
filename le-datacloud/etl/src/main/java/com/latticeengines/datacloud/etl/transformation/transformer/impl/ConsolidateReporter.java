package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.dataflow.transformation.ConsolidateReportFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateReportParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component(ConsolidateReportFlow.TRANSFORMER_NAME)
public class ConsolidateReporter
        extends AbstractDataflowTransformer<ConsolidateReportConfig, ConsolidateReportParameters> {
    private static final Logger log = LoggerFactory.getLogger(ConsolidateReporter.class);

    @Override
    protected boolean validateConfig(ConsolidateReportConfig config, List<String> baseSources) {
        String error = null;
        if (config.getEntity() == null) {
            error = "Entity is required";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected void updateParameters(ConsolidateReportParameters parameters, Source[] baseTemplates,
            Source targetTemplate, ConsolidateReportConfig config, List<String> baseVersions) {
        parameters.setEntity(config.getEntity());
        parameters.setThresholdTime(config.getThresholdTime());
    }

    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, ConsolidateReportParameters paras,
            ConsolidateReportConfig config) {
        List<Pair<String, Class<?>>> columns = prepareSchema();
        Object[][] data = finalizeReport(workflowDir, config.getEntity());
        deleteDataflowAvros(workflowDir);
        uploadAvro(workflowDir, columns, data, config.getEntity());
    }

    private Object[][] finalizeReport(String avroDir, BusinessEntity entity) {
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, avroDir + "/*.avro");
        ObjectNode report = null;
        switch (entity) {
        case Account:
            report = getDefaultAccountReportItems();
            break;
        case Contact:
            report = getDefaultContactReportItems();
            break;
        case Product:
            report = getDefaultProductReportItems();
            break;
        case Transaction:
            report = getDefaultTransactionReportItems();
            break;
        default:
            throw new UnsupportedOperationException(entity.name() + " is not supported in ConsolidateReport yet");
        }
        for (GenericRecord record : records) {
            report.put(record.get(ConsolidateReportFlow.REPORT_TOPIC).toString(),
                    record.get(ConsolidateReportFlow.REPORT_CONTENT).toString());
        }
        ObjectNode finalJson = JsonUtils.createObjectNode();
        finalJson.set(entity.name(), report);
        Object[][] data = new Object[1][1];
        data[0][0] = finalJson.toString();
        return data;
    }

    private ObjectNode getDefaultAccountReportItems() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(ConsolidateReportFlow.REPORT_TOPIC_NEW, "0");
        json.put(ConsolidateReportFlow.REPORT_TOPIC_UPDATE, "0");
        json.put(ConsolidateReportFlow.REPORT_TOPIC_UNMATCH, "0");
        return json;
    }

    private ObjectNode getDefaultContactReportItems() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(ConsolidateReportFlow.REPORT_TOPIC_NEW, "0");
        json.put(ConsolidateReportFlow.REPORT_TOPIC_UPDATE, "0");
        return json;
    }

    private ObjectNode getDefaultProductReportItems() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(ConsolidateReportFlow.REPORT_TOPIC_NEW, "0");
        json.put(ConsolidateReportFlow.REPORT_TOPIC_UPDATE, "0");
        return json;
    }

    private ObjectNode getDefaultTransactionReportItems() {
        ObjectNode json = JsonUtils.createObjectNode();
        json.put(ConsolidateReportFlow.REPORT_TOPIC_NEW, "0");
        return json;
    }

    private void deleteDataflowAvros(String avroDir) {
        try {
            List<String> avros = HdfsUtils.getFilesForDir(yarnConfiguration, avroDir, ".*\\.avro$");
            for (String path : avros) {
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete " + avroDir + "/*.avro", e);
        }
    }

    private List<Pair<String, Class<?>>> prepareSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.ConsolidateReport.name(), String.class));
        return columns;
    }

    private void uploadAvro(String targetDir, List<Pair<String, Class<?>>> schema, Object[][] data,
            BusinessEntity entity) {
        try {
            AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, targetDir, entity.name() + "Report.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getDataFlowBeanName() {
        return ConsolidateReportFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return ConsolidateReportFlow.TRANSFORMER_NAME;
    }

    @Override
    protected Class<ConsolidateReportParameters> getDataFlowParametersClass() {
        return ConsolidateReportParameters.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ConsolidateReportConfig.class;
    }
}
