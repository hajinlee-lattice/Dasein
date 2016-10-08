package com.latticeengines.scoringapi.match.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.service.DbHelper;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.FieldType;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.match.EnrichmentMetadataCache;
import com.latticeengines.scoringapi.match.MatchInputBuilder;
import com.latticeengines.scoringapi.match.Matcher;

public abstract class AbstractMatcher implements Matcher {

    private static final Log log = LogFactory.getLog(AbstractMatcher.class);

    protected static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";

    @Autowired
    protected Warnings warnings;

    @Autowired
    @Qualifier("sqlServerHelper")
    private DbHelper dbHelper;

    @Value("${scoringapi.propdata.shortcircuit:false}")
    protected boolean shouldShortcircuitPropdata;

    @Autowired
    protected RealTimeMatchService realTimeMatchService;

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    protected List<MatchInputBuilder> matchInputBuilders;

    @Autowired
    protected EnrichmentMetadataCache enrichmentMetadataCache;

    @PostConstruct
    public void initialize() throws Exception {
        if (shouldShortcircuitPropdata) {
            log.info("Initialize propdata fetcher executors as scoringapi-propdata shortcircuit is on.");
            dbHelper.initExecutors();
        } else {
            log.info("Skip initialization of propdata fetcher executors "
                    + "via scoringapi as scoringapi-propdata shortcircuit is off.");
        }
    }

    public boolean isAccountMasterBasedModel(ModelSummary modelSummary) {
        return modelSummary.getDataCloudVersion() != null && modelSummary.getDataCloudVersion().startsWith("2.");
    }

    protected MatchInputBuilder getMatchInputBuilder(String matchVersion) {
        for (MatchInputBuilder builder : matchInputBuilders) {
            if (builder.accept(matchVersion)) {
                return builder;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { matchVersion });
    }

    protected void getRecordFromMatchOutput(Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record, //
            MatchInput matchInput, //
            MatchOutput matchOutput) {
        if (matchOutput.getResult().isEmpty()) {
            warnings.addWarning(new Warning(WarningCode.NO_MATCH,
                    new String[] { JsonUtils.serialize(matchInput.getKeyMap()), "No result" }));
        } else {
            List<String> matchFieldNames = matchOutput.getOutputFields();
            OutputRecord outputRecord = matchOutput.getResult().get(0);
            String nameLocationStr = "";
            if (outputRecord.getMatchedNameLocation() != null) {
                nameLocationStr = JsonUtils.serialize(outputRecord.getMatchedNameLocation());
            }
            String errorMessages = outputRecord.getErrorMessages() == null ? ""
                    : Joiner.on(",").join(outputRecord.getErrorMessages());

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "{ 'isMatched':'%s', 'matchedDomain':'%s', 'matchedNameLocation':'%s', 'matchErrors':'%s' }",
                        outputRecord.isMatched(), Strings.nullToEmpty(outputRecord.getMatchedDomain()), nameLocationStr,
                        errorMessages));
            }

            mergeMatchedOutput(matchFieldNames, outputRecord, fieldSchemas, record);
            if (!outputRecord.isMatched()) {
                warnings.addWarning(
                        new Warning(WarningCode.NO_MATCH, new String[] { JsonUtils.serialize(matchInput.getKeyMap()),
                                Strings.nullToEmpty(outputRecord.getMatchedDomain()) + nameLocationStr }));
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(JsonUtils.serialize(record));
        }
    }

    protected void logInDebugMode(String objectType, Object obj) {
        if (log.isDebugEnabled()) {
            log.debug(objectType + JsonUtils.serialize(obj));
        }
    }

    protected String getDataCloudVersion(ModelSummary modelSummary) {
        return modelSummary == null ? null : modelSummary.getDataCloudVersion();
    }

    /*
     * LOGIC
     * 
     * if no enrichment needed
     * 
     * .....then follow regular path
     * 
     * else if enrichment needed
     * 
     * .....if model datacloud version is for account master
     * 
     * .........then follow regular path
     * 
     * .....else if model datacloud version is for RTS
     * 
     * .........then follow regular path but without enrichment
     * 
     * .........and explicitly call account master based matching with
     * ..........enrichment option (without Predefined column selection)
     * 
     */
    protected boolean shouldCallEnrichmentExplicitly(ModelSummary modelSummary, boolean forEnrichment,
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes) {
        boolean shouldCallEnrichmentExplicitly = true;

        if (!forEnrichment || CollectionUtils.isEmpty(selectedLeadEnrichmentAttributes)) {
            shouldCallEnrichmentExplicitly = false;
        } else if (MatchTypeUtil.isValidForAccountMasterBasedMatch(//
                getDataCloudVersion(modelSummary))) {
            shouldCallEnrichmentExplicitly = false;
        }

        return shouldCallEnrichmentExplicitly;
    }

    protected void doEnrichmentPostProcessing(Map<String, Object> record, //
            boolean forEnrichment, MatchInput matchInput, //
            Map<String, Map<String, Object>> resultMap) {
        if (forEnrichment) {
            Map<String, Object> enrichmentData = new HashMap<>();

            List<Column> customSelectionColumns = getCustomSelectionColumns(matchInput);

            if (customSelectionColumns != null) {
                for (Column attr : customSelectionColumns) {
                    if (record.containsKey(attr.getExternalColumnId())) {
                        enrichmentData.put(attr.getExternalColumnId(), //
                                record.get(attr.getExternalColumnId()));
                    }
                }
            }
            resultMap.put(ENRICHMENT, enrichmentData);
        }
    }

    protected MatchInput buildMatchInput(CustomerSpace space, //
            InterpretedFields interpreted, Map<String, Object> record, //
            ModelSummary modelSummary, //
            List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes, //
            boolean skipPredefinedSelection, //
            String overrideDataCloudVersion) {
        MatchInputBuilder matchInputBuilder = //
                getMatchInputBuilder(getDataCloudVersion(modelSummary));
        return matchInputBuilder.buildMatchInput(space, interpreted, //
                record, modelSummary, //
                selectedLeadEnrichmentAttributes, //
                skipPredefinedSelection, //
                overrideDataCloudVersion);
    }

    private List<Column> getCustomSelectionColumns(MatchInput matchInput) {
        List<Column> customSelectionColumns = null;

        if (matchInput.getUnionSelection() != null //
                && matchInput.getUnionSelection().getCustomSelection() != null) {
            customSelectionColumns = matchInput.getUnionSelection().getCustomSelection().getColumns();
        } else if (matchInput.getCustomSelection() != null) {
            customSelectionColumns = matchInput.getCustomSelection().getColumns();
        }

        return customSelectionColumns;
    }

    private void mergeMatchedOutput(List<String> matchFieldNames, //
            OutputRecord outputRecord, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record) {
        List<Object> matchFieldValues = outputRecord.getOutput();

        if (matchFieldNames == null || (matchFieldValues!= null && matchFieldNames.size() != matchFieldValues.size())) {
            throw new LedpException(LedpCode.LEDP_31005,
                    new String[] { String.valueOf(matchFieldNames == null ? "0" : matchFieldNames.size()),
                            matchFieldValues == null ? "0" : String.valueOf(matchFieldValues.size()) });
        }

        for (int i = 0; i < matchFieldNames.size(); i++) {
            String fieldName = matchFieldNames.get(i);
            FieldSchema schema = fieldSchemas.get(fieldName);
            if (schema == null || (schema != null && schema.source != FieldSource.REQUEST)) {
                Object fieldValue = null;
                Object unparsedFieldValue = matchFieldValues==null?null:matchFieldValues.get(i);
                if (schema != null) {
                    fieldValue = unparsedFieldValue == null? null:FieldType.parse(schema.type, unparsedFieldValue);
                } else {
                    fieldValue = unparsedFieldValue;
                }
                record.put(fieldName, fieldValue);
                if (fieldName.equals(IS_PUBLIC_DOMAIN)) {
                    Boolean isPublicDomain = (Boolean) fieldValue;
                    if (isPublicDomain) {
                        warnings.addWarning(new Warning(WarningCode.PUBLIC_DOMAIN,
                                new String[] { Strings.nullToEmpty(outputRecord.getMatchedDomain()) }));
                    }
                }
            }
        }
    }

}
