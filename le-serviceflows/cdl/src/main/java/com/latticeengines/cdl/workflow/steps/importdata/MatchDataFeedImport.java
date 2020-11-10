package com.latticeengines.cdl.workflow.steps.importdata;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DUNS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportFileSignature;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;

public class MatchDataFeedImport extends BaseSparkStep<ImportDataFeedTaskConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchDataFeedImport.class);

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    private BusinessEntity entity;

    @Value("${cdl.pa.use.directplus}")
    private boolean useDirectPlus;

    @Override
    public void execute() {
        log.info("Executing spark step " + getClass().getSimpleName());
        if (skipMatch()) {
            log.info("Match condition not meet, skip match!");
            return;
        }
        customerSpace = parseCustomerSpace(configuration);
        List<String> inputAvroPathList = getInputAvroPathList();
        if (CollectionUtils.isNotEmpty(inputAvroPathList)) {
            List<MatchCommand> matchCommandList = new ArrayList<>();
            for (String avroPath: inputAvroPathList) {
                MatchInput input = constructMatchInput(avroPath);
                if (!OperationalMode.isEntityMatch(input.getOperationalMode())) {
                    input.setDataCloudOnly(true);
                }
                MatchCommand command = bulkMatchService.match(input, null);
                log.info("Bulk match finished: {}", JsonUtils.serialize(command));
                matchCommandList.add(command);

            }
            postMatchProcessing(matchCommandList);
        }
    }

    private boolean skipMatch() {
        if (StringUtils.isEmpty(configuration.getDataFeedTaskId())) {
            log.error("Import DataFeedTaskId is empty!");
            return true;
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(),
                configuration.getDataFeedTaskId());
        if (!BusinessEntity.Account.name().equalsIgnoreCase(dataFeedTask.getEntity()) &&
                !BusinessEntity.Contact.name().equalsIgnoreCase(dataFeedTask.getEntity())) {
            log.info("The import is not Account or Contact, skip match import.");
            return true;
        }
        entity = BusinessEntity.getByName(dataFeedTask.getEntity());
        log.info("Import match entity: " + entity.name());
        ImportFileSignature signature = getObjectFromContext(IMPORT_FILE_SIGNATURE, ImportFileSignature.class);
        if (!(signature.getHasCompanyName() || signature.getHasDomain() || signature.getHasDUNS())) {
            log.info("Import file does not have CompanyName or DUNS or Website. Skip match");
            return true;
        }
        return false;
    }

    private List<String> getInputAvroPathList() {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        if (applicationId == null) {
            log.error("Cannot find EAI import job application!");
            return null;
        }
        EaiImportJobDetail importJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (importJobDetail == null || CollectionUtils.isEmpty(importJobDetail.getPathDetail())) {
            log.error("EAI import job {} not exists, or cannot find import data.", applicationId);
            return null;
        }
        return importJobDetail.getPathDetail();
    }

    protected void postMatchProcessing(List<MatchCommand> commandList) {
        String customer = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String finalResultTable = NamingUtils.timestamp("DataFeedImportMatchResult");

        bulkMatchService.registerResultTable(customer, commandList, finalResultTable);
        saveOutputValue(WorkflowContextConstants.Outputs.MATCH_RESULT_TABLE_NAME, finalResultTable);
    }

    private MatchInput constructMatchInput(String avroDir) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        Set<String> inputFields = getInputFields(avroDir);
        Map<MatchKey, List<String>> keyMap = getKeyMap(inputFields);
        matchInput.setKeyMap(keyMap);
        matchInput.setSkipKeyResolution(true);
        matchInput.setTargetEntity(BusinessEntity.LatticeAccount.name());
        matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);
        matchInput.setMatchDebugEnabled(false);
        matchInput.setCustomSelection(getColumnSelection());

        matchInput.setPartialMatchEnabled(true);
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setUseDirectPlus(useDirectPlus);

        return matchInput;
    }

    private ColumnSelection getColumnSelection() {
        List<Column> columns = Collections.singletonList(new Column(ATTR_LDC_DUNS));
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        return columnSelection;
    }

    protected Map<MatchKey, List<String>> getKeyMap(Set<String> inputFields) {
        Map<MatchKey, String> potentialKeyMap = new HashMap<>(MatchKey.LDC_MATCH_KEY_STD_FLDS);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        potentialKeyMap.forEach((key, col) -> {
            if (inputFields.contains(col)) {
                keyMap.put(key, Collections.singletonList(col));
            }
        });
        if (BusinessEntity.Contact.equals(entity) && inputFields.contains(InterfaceName.Email.name())) {
            List<String> domains = new ArrayList<>();
            domains.add(InterfaceName.Email.name());
            if (keyMap.containsKey(MatchKey.Domain)) {
                domains.addAll(keyMap.get(MatchKey.Domain));
            }
            keyMap.put(MatchKey.Domain, domains);
        }
        return keyMap;
    }

    private Set<String> getInputFields(String avroDir) {
        String avroGlob = PathUtils.toAvroGlob(avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
    }
}
