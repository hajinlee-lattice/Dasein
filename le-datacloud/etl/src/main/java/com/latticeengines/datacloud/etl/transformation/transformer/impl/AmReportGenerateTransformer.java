package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.AccMastrManChkReportGenFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.AmReportGenerateParams;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccMastrManChkReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.monitor.SlackSettings;
import com.latticeengines.domain.exposed.monitor.SlackSettings.Color;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.monitor.exposed.service.SlackService;

@Component(AccMastrManChkReportGenFlow.TRANSFORMER_NAME)
public class AmReportGenerateTransformer
        extends AbstractDataflowTransformer<AccMastrManChkReportConfig, AmReportGenerateParams> {

    private static final Logger log = LoggerFactory.getLogger(AmReportGenerateTransformer.class);
    private static final String SLACK_BOT = AccMastrManChkReportGenFlow.TRANSFORMER_NAME;
    private static final Color SLACK_COLOR_DANGER = Color.DANGER;

    @Autowired
    private EmailService emailService;

    @Autowired
    private SlackService slackService;

    //TODO: remove default value after fixing properties file
    @Value("${datacloud.notification.email:}")
    private String email;

    @Value("${datacloud.slack.webhook.url}")
    private String slackWebHookUrl;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    @Override
    protected String getDataFlowBeanName() {
        return AccMastrManChkReportGenFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return AccMastrManChkReportGenFlow.TRANSFORMER_NAME;
    }

    @Override
    protected Class<AmReportGenerateParams> getDataFlowParametersClass() {
        return AmReportGenerateParams.class;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AccMastrManChkReportConfig.class;
    }

    @Override
    protected void updateParameters(AmReportGenerateParams parameters, Source[] baseSources, Source targetTemplate,
            AccMastrManChkReportConfig config, List<String> baseVersions) {
        parameters.setCheckCode(config.getCheckCode());
        parameters.setRowId(config.getRowId());
        parameters.setCheckField(config.getCheckField());
        parameters.setGroupId(config.getGroupId());
        parameters.setCheckValue(config.getCheckValue());
        parameters.setCheckMessage(config.getCheckMessage());
        Schema schema = hdfsSourceEntityMgr.getAvscSchemaAtVersion(baseSources[0], baseVersions.get(0));
        String count = schema.getProp("Count");
        if (count != null) {
            parameters.setSrcCount(Long.parseLong(count));
        } else {
            parameters.setSrcCount(hdfsSourceEntityMgr.count(baseSources[0], baseVersions.get(0)));
        }
    }

    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, AmReportGenerateParams parameters,
            AccMastrManChkReportConfig config) {
        try {
            Long count = step.getCount();
            if (count == null) {
                count = AvroUtils.count(yarnConfiguration, workflowDir + "/*.avro");
            }
            if (count > 0 || parameters.getSrcCount() > 0) {
                log.info("Send slack notification");
                if (StringUtils.isNotEmpty(slackWebHookUrl)) {
                    String title = "AccountMaster validation failed";
                    String message = String.format("Please check hive table %s for reference", "LDC_"
                            + step.getTarget().getSourceName() + "_" + step.getTargetVersion().replace("-", "_"));
                    String pretext = "[" + leEnv + "-" + leStack + "]";
                    slackService.sendSlack(
                            new SlackSettings(slackWebHookUrl, title, pretext, message, SLACK_BOT, SLACK_COLOR_DANGER));
                }
                if (StringUtils.isNotBlank(email)) {
                    log.info("Send email notification");
                    emailService.sendSimpleEmail("AccountMaster validation failed",
                            String.format("Please check hive table %s for reference",
                                    "LDC_" + step.getTarget().getSourceName() + "_"
                                            + step.getTargetVersion().replace("-", "_")),
                            "text/plain", Collections.singleton(email));
                }
            }
        } catch (Exception e) {
            log.error("Failed to update parameters", e);
        }
    }

}
