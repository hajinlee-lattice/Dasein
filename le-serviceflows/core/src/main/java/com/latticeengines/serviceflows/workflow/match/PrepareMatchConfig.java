package com.latticeengines.serviceflows.workflow.match;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.IOBufferType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("preMatchStep")
public class PrepareMatchConfig extends BaseWorkflowStep<MatchStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchConfig.class);
    private static final Map<MatchKey, String> MATCH_KEYS_TO_DISPLAY_NAMES = new HashMap<>();

    static {
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Name, InterfaceName.CompanyName.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.City, InterfaceName.City.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.State, InterfaceName.State.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Country, InterfaceName.Country.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.Zipcode, InterfaceName.PostalCode.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.PhoneNumber, InterfaceName.PhoneNumber.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.DUNS, InterfaceName.DUNS.name());
        MATCH_KEYS_TO_DISPLAY_NAMES.put(MatchKey.ExternalId, InterfaceName.Id.name());
    }

    @Autowired
    private MatchProxy matchProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void execute() {
        log.info("Inside PreMatchStep execute()");
        Table preMatchEventTable = preMatchEventTable();
        putObjectInContext(PREMATCH_EVENT_TABLE, preMatchEventTable);
        MatchInput input = prepareMatchInput(preMatchEventTable);
        BulkMatchWorkflowConfiguration configuration = matchProxy.getBulkConfig(input,
                getConfiguration().getMatchHdfsPod());
        putObjectInContext(BulkMatchWorkflowConfiguration.class.getName(), configuration);
    }

    @Override
    public void skipStep() {
        log.info("Skip matching step and register event table now.");
        Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getInputTableName());
        putObjectInContext(EVENT_TABLE, table);
        putObjectInContext(MATCH_RESULT_TABLE, table);
        log.info("Skip embedded bulk match workflow.");
        skipEmbeddedWorkflow(BulkMatchWorkflowConfiguration.class);
    }

    private Table preMatchEventTable() {
        Table preMatchEventTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getInputTableName());
        preMatchEventTable.setName(preMatchEventTable.getName() + "_" + System.currentTimeMillis());
        return preMatchEventTable;
    }

    private MatchInput prepareMatchInput(Table preMatchEventTable) {
        MatchInput matchInput = new MatchInput();
        matchInput.setYarnQueue(getConfiguration().getMatchQueue());

        if (getConfiguration().getCustomizedColumnSelection() == null
                && getConfiguration().getPredefinedColumnSelection() == null) {
            throw new RuntimeException("Must specify either CustomizedColumnSelection or PredefinedColumnSelection");
        }

        ColumnSelection.Predefined predefined = getConfiguration().getPredefinedColumnSelection();
        boolean retainLatticeAccountId = getConfiguration().isRetainLatticeAccountId();
        if (predefined != null) {
            if (retainLatticeAccountId) {
                log.info("Retaining Lattice Account Id in the match result.");
                ColumnSelection columnSelection = new ColumnSelection();
                List<Column> columns = Arrays.asList(new Column(InterfaceName.LatticeAccountId.name()));
                columnSelection.setColumns(columns);
                UnionSelection unionSelection = new UnionSelection();
                unionSelection.setCustomSelection(columnSelection);
                Map<Predefined, String> map = new HashMap<>();
                map.put(predefined, "1.0");
                unionSelection.setPredefinedSelections(map);
                matchInput.setUnionSelection(unionSelection);
            } else {
                matchInput.setPredefinedSelection(predefined);
            }

            String version = getConfiguration().getPredefinedSelectionVersion();
            if (StringUtils.isEmpty(version)) {
                version = "1.0";
                getConfiguration().setPredefinedSelectionVersion(version);
            }

            putStringValueInContext(MATCH_PREDEFINED_SELECTION, predefined.getName());
            putStringValueInContext(MATCH_PREDEFINED_SELECTION_VERSION, version);

            log.info("Using predefined column selection " + predefined + " at version " + version);
        } else {
            matchInput.setCustomSelection(getConfiguration().getCustomizedColumnSelection());

            putObjectInContext(MATCH_CUSTOMIZED_SELECTION, getConfiguration().getCustomizedColumnSelection());

        }
        matchInput.setDataCloudVersion(getConfiguration().getDataCloudVersion());
        log.info("Using Data Cloud Version = " + getConfiguration().getDataCloudVersion());

        matchInput.setRequestSource(getConfiguration().getMatchRequestSource());

        matchInput.setTenant(new Tenant(configuration.getCustomerSpace().toString()));
        matchInput.setOutputBufferType(IOBufferType.AVRO);

        Map<MatchKey, List<String>> matchInputKeys = new HashMap<>();
        if (configuration.getSourceSchemaInterpretation() != null && configuration.getSourceSchemaInterpretation()
                .equals(SchemaInterpretation.SalesforceAccount.toString())) {
            if (preMatchEventTable.getAttribute(InterfaceName.Website.name()) == null
                    || (preMatchEventTable.getAttribute(InterfaceName.Website.name()).getApprovedUsage() != null
                            && preMatchEventTable.getAttribute(InterfaceName.Website.name()).getApprovedUsage()
                                    .contains(ApprovedUsage.IGNORED.getName()))) {
                matchInputKeys.put(MatchKey.Domain, new ArrayList<>());
            } else {
                matchInputKeys.put(MatchKey.Domain, Collections.singletonList(InterfaceName.Website.name()));
            }
        } else if (configuration.getSourceSchemaInterpretation() != null && configuration
                .getSourceSchemaInterpretation().equals(SchemaInterpretation.SalesforceLead.toString())) {
            if (preMatchEventTable.getAttribute(InterfaceName.Email.name()) == null
                    || (preMatchEventTable.getAttribute(InterfaceName.Email.name()).getApprovedUsage() != null
                            && preMatchEventTable.getAttribute(InterfaceName.Email.name()).getApprovedUsage()
                                    .contains(ApprovedUsage.IGNORED.getName()))) {
                matchInputKeys.put(MatchKey.Domain, new ArrayList<>());
            } else {
                matchInputKeys.put(MatchKey.Domain, Collections.singletonList(InterfaceName.Email.name()));
            }
            if (preMatchEventTable.getAttribute(InterfaceName.PhoneNumber.name()) != null) {
                matchInputKeys.put(MatchKey.PhoneNumber, new ArrayList<>());
                MATCH_KEYS_TO_DISPLAY_NAMES.remove(MatchKey.PhoneNumber);
            }
        }
        for (MatchKey matchKey : MATCH_KEYS_TO_DISPLAY_NAMES.keySet()) {
            if (preMatchEventTable.getAttribute(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey)) == null
                    || (preMatchEventTable.getAttribute(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey)) != null
                            && preMatchEventTable.getAttribute(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey))
                                    .getApprovedUsage() != null
                            && preMatchEventTable.getAttribute(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey))
                                    .getApprovedUsage().contains(ApprovedUsage.IGNORED.getName()))) {
                matchInputKeys.put(matchKey, new ArrayList<>());
            } else {
                log.info(String.format("attribute: %s is found as: %s", matchKey, JsonUtils
                        .serialize(preMatchEventTable.getAttribute(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey)))));
                matchInputKeys.put(matchKey, Arrays.asList(MATCH_KEYS_TO_DISPLAY_NAMES.get(matchKey)));
            }
        }
        matchInput.setKeyMap(matchInputKeys);
        matchInput.setPrepareForDedupe(!getConfiguration().isSkipDedupe());

        String avroDir = ExtractUtils.getSingleExtractPath(yarnConfiguration, preMatchEventTable);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        inputBuffer.setTableName(preMatchEventTable.getName());

        Schema providedSchema;
        try {
            providedSchema = TableUtils.createSchema(preMatchEventTable.getName(), preMatchEventTable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create avro schema from pre-match event table.", e);
        }

        Schema extractedSchema;
        try {
            String avroGlob;
            if (avroDir.endsWith(".avro")) {
                avroGlob = avroDir;
            } else {
                avroGlob = avroDir.endsWith("/") ? avroDir + "*.avro" : avroDir + "/*.avro";
            }
            extractedSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract avro schema from input avro.", e);
        }

        Schema schema = AvroUtils.alignFields(providedSchema, extractedSchema);

        inputBuffer.setSchema(schema);

        matchInput.setInputBuffer(inputBuffer);

        matchInput.setExcludePublicDomain(getConfiguration().isExcludePublicDomain());

        matchInput.setPublicDomainAsNormalDomain(getConfiguration().isPublicDomainAsNormalDomain());

        return matchInput;
    }

}
