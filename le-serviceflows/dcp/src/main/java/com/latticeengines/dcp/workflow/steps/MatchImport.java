package com.latticeengines.dcp.workflow.steps;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.serviceflows.workflow.match.BaseMatchStep;

@Lazy
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchImport extends BaseMatchStep<ImportSourceStepConfiguration> {

    @Inject
    private UploadProxy uploadProxy;

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
    protected void preMatchProcessing(MatchInput matchInput) {
        String uploadId = configuration.getUploadId();
        CustomerSpace customerSpace = configuration.getCustomerSpace();

        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.MATCH_STARTED, null);

        matchInput.setUseDirectPlus(true);
        matchInput.setDplusMatchConfig(configuration.getMatchConfig());
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);

        List<String> columnIds = getDCPEnrichAttrs();
        List<Column> columns = columnIds.stream().map(c -> new Column(c, c)).collect(Collectors.toList());
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        matchInput.setCustomSelection(columnSelection);
    }

    //FIXME: in alpha release, use a hard coded enrich list
    private List<String> getDCPEnrichAttrs() {
        return Arrays.asList(
                "PrimaryBusinessName",
                "TradeStyleName",
                "PrimaryAddressStreetLine1",
                "PrimaryAddressStreetLine2",
                "PrimaryAddressLocalityName",
                "PrimaryAddressRegionName",
                "PrimaryAddressPostalCode",
                "PrimaryAddressCountyName",
                "TelephoneNumber",
                "IndustryCodeUSSicV4Code"
        );
    }

}
