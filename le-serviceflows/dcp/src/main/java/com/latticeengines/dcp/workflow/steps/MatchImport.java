package com.latticeengines.dcp.workflow.steps;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.BaseMatchStep;

@Lazy
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchImport extends BaseMatchStep<ImportSourceStepConfiguration> {

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
        matchInput.setUseDirectPlus(true);
        matchInput.setTargetEntity(BusinessEntity.PrimeAccount.name());
        matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);
        // matchInput.setExcludePublicDomain(false); // not sure if needed
        // matchInput.setPartialMatchEnabled(true); // not sure if needed

        List<String> columnIds = getDCPEnrichAttrs();
        List<Column> columns = columnIds.stream().map(c -> new Column(c, c)).collect(Collectors.toList());
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        matchInput.setCustomSelection(columnSelection);
    }

    //FIXME: in alpha release, use a hard coded enrich list
    private List<String> getDCPEnrichAttrs() {
        return Arrays.asList(
                DataCloudConstants.ATTR_LDC_DUNS,
                DataCloudConstants.ATTR_LDC_NAME,
                "TRADESTYLE_NAME",
                "LDC_Street",
                "STREET_ADDRESS_2",
                DataCloudConstants.ATTR_CITY,
                DataCloudConstants.ATTR_STATE,
                DataCloudConstants.ATTR_ZIPCODE,
                DataCloudConstants.ATTR_COUNTRY,
                "TELEPHONE_NUMBER",
                "LE_SIC_CODE"
        );
    }

}
