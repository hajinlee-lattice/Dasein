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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataFeedTaskConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;

public class MatchDataFeedImport extends BaseSparkStep<ImportDataFeedTaskConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchDataFeedImport.class);

    static final Map<MatchKey, String> MATCH_KEYS_TO_DISPLAY_NAMES = new HashMap<>();
    static {
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Name, InterfaceName.CompanyName.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.DUNS, InterfaceName.DUNS.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Domain, InterfaceName.Website.name());
    }

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Override
    public void execute() {
        log.info("Executing spark step " + getClass().getSimpleName());
        if (skipMatch()) {
            log.info("Import file does not have CompanyName or DUNS or Website. Skip match");
            return;
        }
        customerSpace = parseCustomerSpace(configuration);
        List<String> inputAvroPathList = getInputAvroPathList();
        if (CollectionUtils.isNotEmpty(inputAvroPathList)) {
            List<MatchCommand> matchCommandList = new ArrayList<>();
            for (String avroPath: inputAvroPathList) {
                MatchInput input = constructMatchInput(avroPath);
                preMatchProcessing(input);
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
        ImportFileSignature signature = getObjectFromContext(IMPORT_FILE_SIGNATURE, ImportFileSignature.class);
        return !(signature.getHasCompanyName() || signature.getHasDomain() || signature.getHasDUNS());
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

    private void preMatchProcessing(MatchInput matchInput) {
        matchInput.setTargetEntity(BusinessEntity.LatticeAccount.name());
        matchInput.setRequestSource(MatchRequestSource.ENRICHMENT);
        matchInput.setMatchDebugEnabled(false);

        List<Column> columns = Collections.singletonList(new Column(ATTR_LDC_DUNS));
        ColumnSelection columnSelection = new ColumnSelection();
        columnSelection.setColumns(columns);
        matchInput.setCustomSelection(columnSelection);
    }

    protected void postMatchProcessing(List<MatchCommand> commandList) {
        String customer = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        String finalResultTable = NamingUtils.timestamp("DataFeedImportMatchResult");

        bulkMatchService.registerResultTable(customer, commandList, finalResultTable);
//        putStringValueInContext(MATCH_RESULT_TABLE_NAME, finalResultTable);
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
        return matchInput;
    }

    protected Map<MatchKey, List<String>> getKeyMap(Set<String> inputFields) {
        Map<MatchKey, String> potentialKeyMap = new HashMap<>(MATCH_KEYS_TO_DISPLAY_NAMES);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        potentialKeyMap.forEach((key, col) -> {
            if (inputFields.contains(col)) {
                keyMap.put(key, Collections.singletonList(col));
            }
        });
        return keyMap;
    }

    private Set<String> getInputFields(String avroDir) {
        String avroGlob = PathUtils.toAvroGlob(avroDir);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
    }
}
