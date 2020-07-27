package com.latticeengines.dcp.workflow.steps;

import static com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration.MATCH_PERCENTAGE;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.MatchRuleProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.match.BaseMatchStep;

@Lazy
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchImport extends BaseMatchStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchImport.class);

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private MatchRuleProxy matchRuleProxy;

    @Override
    protected String getInputAvroPath() {
        String inputPath = getStringValueFromContext(IMPORT_DATA_LOCATION);
        if (StringUtils.isBlank(inputPath)) {
            throw new IllegalStateException("Cannot find import data location from context");
        }
        return inputPath;
    }

    @Override
    protected String getResultTableName() {
        return NamingUtils.timestamp("MatchResult");
    }

    @Override
    protected boolean saveToParquet() {
        return true;
    }

    @Override
    protected void preMatchProcessing(MatchInput matchInput) {
        String uploadId = configuration.getUploadId();
        CustomerSpace customerSpace = configuration.getCustomerSpace();

        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.MATCH_STARTED, null);

        MatchRuleConfiguration matchRuleConfiguration = matchRuleProxy.getMatchConfig(customerSpace.toString(), configuration.getSourceId());

        log.info("MatchRuleConfiguration of source " + configuration.getSourceId() + " : " + JsonUtils.serialize(matchRuleConfiguration));

        log.info("After translate into DplusMatchConfig : " + JsonUtils.serialize(configuration.getMatchConfig()));

        matchInput.setUseDirectPlus(true);
        matchInput.setDplusMatchConfig(configuration.getMatchConfig());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);

        List<String> columnIds = configuration.getAppendConfig().getElementIds();
        List<Column> columns = columnIds.stream().map(c -> new Column(c, c)).collect(Collectors.toList());
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        matchInput.setCustomSelection(columnSelection);
    }

    @Override
    protected void matchCompleted(MatchInput input, MatchCommand command){
        String uploadId = configuration.getUploadId();
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        uploadProxy.updateProgressPercentage(customerSpace.toString(), uploadId, MATCH_PERCENTAGE);
    }

}
