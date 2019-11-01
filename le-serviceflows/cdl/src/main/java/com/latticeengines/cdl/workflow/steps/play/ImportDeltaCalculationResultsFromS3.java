package com.latticeengines.cdl.workflow.steps.play;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.ImportDeltaCalculationResultsFromS3StepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.serviceflows.workflow.export.BaseImportExportS3;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;

@Component("importDeltaCalculationResultsFromS3")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDeltaCalculationResultsFromS3
        extends BaseImportExportS3<ImportDeltaCalculationResultsFromS3StepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportDeltaCalculationResultsFromS3.class);

    @Inject
    private PlayProxy playProxy;

    @Override
    protected void buildRequests(List<ImportExportRequest> requests) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        String playId = configuration.getPlayId();
        String launchId = configuration.getLaunchId();
        log.info(String.format("Building requests for tenant=%s, playId=%s launchId=%s", customerSpace.getTenantId(),
                playId, launchId));

        List<String> tables = getMetadataTableNames(customerSpace, playId, launchId);
        if (CollectionUtils.isNotEmpty(tables)) {
            tables.forEach(tblName -> {
                Table table = metadataProxy.getTable(customerSpace.toString(), tblName);
                if (table == null) {
                    throw new RuntimeException("Table " + tblName + " for customer " //
                            + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                            + " in attr repo does not exists.");
                }
                addTableToRequestForImport(table, requests);
            });
        } else {
            log.error(
                    String.format("There is no metadat tables associated with tenant %s", customerSpace.getTenantId()));
        }

    }

    private List<String> getMetadataTableNames(CustomerSpace customerSpace, String playId, String launchId) {
        PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace.getTenantId(), playId, launchId);
        if (playLaunch == null) {
            throw new NullPointerException("PlayLaunch should not be null");
        }

        List<String> tableNames = new ArrayList<>();
        int totalDfs = 0;
        String addAccounts = playLaunch.getAddAccountsTable();
        String addContacts = playLaunch.getAddContactsTable();
        String delAccounts = playLaunch.getRemoveAccountsTable();
        String delContacts = playLaunch.getRemoveContactsTable();
        String completeContacts = playLaunch.getCompleteContactsTable();

        boolean addAccountsExists = StringUtils.isNotEmpty(addAccounts);
        boolean delAccountsExists = StringUtils.isNotEmpty(delAccounts);
        // 1. add csv dataframe
        if (StringUtils.isNotEmpty(addContacts)) {
            if (addAccountsExists) {
                totalDfs += 1;
                tableNames.add(addAccounts);
                tableNames.add(addContacts);
            } else {
                throw new RuntimeException("No corresponding Account table exists for add csv dataframe.");
            }
        } else {
            if (addAccountsExists) {
                totalDfs += 1;
                tableNames.add(addAccounts);
            }
        }

        // 2. recommendation dataframe
        if (StringUtils.isNotEmpty(completeContacts)) {
            if (addAccountsExists) {
                totalDfs += 1;
                tableNames.add(completeContacts);
            } else {
                throw new RuntimeException("No corresponding Account table exists for recommendation dataframe.");
            }
        }

        // 3. delete csv dataframe
        if (StringUtils.isNotEmpty(delContacts)) {
            if (delAccountsExists) {
                totalDfs += 1;
                tableNames.add(delAccounts);
                tableNames.add(delContacts);
            } else {
                throw new RuntimeException("No corresponding Account table exists for delete csv dataframe.");
            }
        } else {
            if (delAccountsExists) {
                totalDfs += 1;
                tableNames.add(delAccounts);
            }
        }
        log.info(String.format("totalDfs=%d, tableNames=%s", totalDfs, Arrays.toString(tableNames.toArray())));
        putStringValueInContext(DeltaCampaignLaunchWorkflowConfiguration.DATA_FRAME_NUM, String.valueOf(totalDfs));

        return tableNames;
    }

}
