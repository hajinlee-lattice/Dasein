package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;

@Component(MatchRawStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchRawStream extends BaseActivityStreamStep<ProcessActivityStreamStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MatchRawStream.class);

    static final String BEAN_NAME = "matchRawStream";

    // matched table of merged imports
    private static final String RAWSTREAM_MATCHED_PREFIX_FORMAT = "Matched_RawStream_%s";
    // matched table merged with existing batch store, used as input of rematch step
    private static final String RAWSTREAM_PRE_REMATCH_TABLE_PREFIX_FORMAT = "PreReMatch_RawStream_%s";
    // rematched table of merged imports + active batch store
    private static final String RAWSTREAM_REMATCHED_TABLE_PREFIX_FORMAT = "ReMatched_RawStream_%s";
    private static final String RAWSTREAM_NEW_ACC_TABLE_PERFIX_FORMAT = "NewAcc_RawStream_%s";
    private static final List<String> RAWSTREAM_PARTITION_KEYS = ImmutableList.of(InterfaceName.__StreamDateId.name());

    // streamId -> set of column names
    private final Map<String, Set<String>> streamImportColumnNames = new HashMap<>();
    // streamId -> table prefix of matched raw streams imports processed by
    // transformation request
    private final Map<String, String> matchedTablePrefixes = new HashMap<>();
    // streamId -> table name of accounts created by matching raw stream
    private final Map<String, String> newAccountTables = new HashMap<>();
    private long paTimestamp;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        paTimestamp = getLongValueFromContext(PA_TIMESTAMP);
        log.info("Timestamp used as current time to build raw stream = {}", paTimestamp);
        log.info("IsRematch={}, isReplace={}", configuration.isRematchMode(), configuration.isReplaceMode());
        buildStreamImportColumnNames();
        bumpEntityMatchStagingVersion();
    }

    @Override
    protected void onPostTransformationCompleted() {
        if (isShortCutMode()) {
            log.warn("Should not reach post transformation callback in shortcut mode");
            return;
        }
        Map<String, String> matchedTablenames = getFullTablenames(matchedTablePrefixes);
        log.info("Matched raw stream tables, tables={}, pipelineVersion={}", matchedTablenames, pipelineVersion);
        putObjectInContext(ENTITY_MATCH_STREAM_TARGETTABLE, matchedTablenames);

        // check if any new accounts generated
        addNewAccountTablesToCtx();
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        if (isShortCutMode()) {
            log.info("In shortcut mode, skip matching raw activity stream");
            return null;
        }

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName(BEAN_NAME);

        List<TransformationStepConfig> steps = new ArrayList<>();

        /*-
         * normal steps:
         * 1. merge imports
         * 2. match against current universe
         */
        Map<String, Integer> matchedImportTableIdx = configuration.getStreamImports().entrySet() //
                .stream() //
                .map(entry -> {
                    String streamId = entry.getKey();
                    List<String> importTables = entry.getValue() //
                            .stream() //
                            .map(ActivityImport::getTableName) //
                            .collect(Collectors.toList());
                    if (CollectionUtils.isEmpty(importTables)) {
                        return null;
                    }

                    // add concat/match step
                    return Pair.of(streamId, concatAndMatchStreamImports(streamId, importTables, steps));
                }) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        /*-
         * additional rematch steps:
         * 1. merge matched imports with batch store (if exist) and treat as a huge import
         * 2. rematch against empty universe
         */
        if (configuration.isRematchMode()) {
            log.info("Adding rematch steps, matchedImportTableIdx = {}", matchedImportTableIdx);
            configuration.getActivityStreamMap().forEach((streamId, stream) -> {
                Integer matchedStepIdx = matchedImportTableIdx.get(streamId);
                String activeTable = getRawStreamActiveTable(streamId, stream);
                appendRawStream(steps, stream, paTimestamp, matchedStepIdx, activeTable,
                        RAWSTREAM_PRE_REMATCH_TABLE_PREFIX_FORMAT).ifPresent(pair -> {
                            int appendStepIdx = pair.getValue();
                            // input table columns = [ import columns ] union [ active batch store columns ]
                            Set<String> columns = new HashSet<>(
                                    streamImportColumnNames.getOrDefault(streamId, new HashSet<>()));
                            if (activeTable != null) {
                                columns.addAll(getTableColumnNames(activeTable));
                            }
                            addMatchStep(stream, columns, steps, appendStepIdx, true);
                        });
            });
        }

        if (CollectionUtils.isEmpty(steps)) {
            log.info("No existing/new activity stream found, skip match raw stream step");
            return null;
        }

        request.setSteps(steps);
        return request;
    }

    private boolean isShortCutMode() {
        return Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
    }

    // add all new account tables that exist to context (means there are new
    // accounts)
    private void addNewAccountTablesToCtx() {
        Map<String, String> newAccTablesWithData = newAccountTables.entrySet() //
                .stream() //
                .filter(entry -> {
                    Table newAccountTable = metadataProxy.getTableSummary(customerSpace.toString(), entry.getValue());
                    return newAccountTable != null;
                }) //
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("New account tables (with data) for raw stream = {}", newAccTablesWithData);
        putObjectInContext(ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE, newAccTablesWithData);
    }

    private Integer concatAndMatchStreamImports(@NotNull String streamId, @NotNull List<String> importTables,
            @NotNull List<TransformationStepConfig> steps) {
        AtlasStream stream = configuration.getActivityStreamMap().get(streamId);
        Set<String> importTableColumns = streamImportColumnNames.get(streamId);
        Preconditions.checkNotNull(streamId, String.format("Stream %s is not in config", streamId));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(stream.getMatchEntities()),
                String.format("Stream %s does not have match entities", streamId));

        // concat import
        steps.add(dedupAndConcatTables(null, false, importTables));
        addMatchStep(stream, importTableColumns, steps, steps.size() - 1, false);
        return steps.size() - 1;
    }

    private void addMatchStep(@NotNull AtlasStream stream, @NotNull Set<String> importTableColumns,
            @NotNull List<TransformationStepConfig> steps, int matchInputTableIdx, boolean isRematchMode) {
        String tablePrefixFormat = isRematchMode ? RAWSTREAM_REMATCHED_TABLE_PREFIX_FORMAT
                : RAWSTREAM_MATCHED_PREFIX_FORMAT;
        String matchedTablePrefix = String.format(tablePrefixFormat, stream.getStreamId());
        String newAccTableName = NamingUtils
                .timestamp(String.format(RAWSTREAM_NEW_ACC_TABLE_PERFIX_FORMAT, stream.getStreamId()));
        String matchConfig;
        MatchInput baseMatchInput = getBaseMatchInput();
        if (isRematchMode) {
            setServingVersionForEntityMatchTenant(baseMatchInput);
            log.info("Specifying serving version for rematch mode, servingVersion = {}",
                    baseMatchInput.getServingVersion());
        }
        // match entity TODO maybe support entity match GA?
        if (stream.getMatchEntities().contains(Contact.name())) {
            // contact match
            matchConfig = MatchUtils.getAllocateIdMatchConfigForContact(customerSpace.toString(), baseMatchInput,
                    importTableColumns, getSystemIds(BusinessEntity.Account), getSystemIds(BusinessEntity.Contact),
                    newAccTableName, isRematchMode, false);
        } else if (stream.getMatchEntities().contains(Account.name())) {
            // account match
            matchConfig = MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), baseMatchInput,
                    importTableColumns, getSystemIds(BusinessEntity.Account), newAccTableName, isRematchMode);
        } else {
            log.error("Match entities {} in stream {} is not supported", stream.getMatchEntities(),
                    stream.getStreamId());
            throw new UnsupportedOperationException(
                    String.format("Match entities in stream %s are not supported", stream.getStreamId()));
        }
        // record the final table prefix for stream
        matchedTablePrefixes.put(stream.getStreamId(), matchedTablePrefix);
        newAccountTables.put(stream.getStreamId(), newAccTableName);
        steps.add(match(matchInputTableIdx, matchedTablePrefix, matchConfig));
    }

    private void buildStreamImportColumnNames() {
        Map<String, List<ActivityImport>> streamImports = configuration.getStreamImports();
        if (MapUtils.isEmpty(streamImports)) {
            return;
        }

        streamImports.forEach((streamId, imports) -> {
            String[] importTables = imports.stream().map(ActivityImport::getTableName).toArray(String[]::new);
            Set<String> columns = getTableColumnNames(importTables);
            log.info("Stream {} has {} imports, {} total columns", streamId, imports.size(), columns.size());
            streamImportColumnNames.put(streamId, columns);
        });
    }
}
