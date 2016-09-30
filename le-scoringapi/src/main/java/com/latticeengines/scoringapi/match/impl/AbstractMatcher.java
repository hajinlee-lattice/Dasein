package com.latticeengines.scoringapi.match.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.impl.RealTimeMatchFetcher;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.scoringapi.match.EnrichmentMetadataCache;
import com.latticeengines.scoringapi.match.MatchInputBuilder;
import com.latticeengines.scoringapi.match.Matcher;

public abstract class AbstractMatcher implements Matcher, ApplicationContextAware {

    private static final Log log = LogFactory.getLog(AbstractMatcher.class);

    protected static final String IS_PUBLIC_DOMAIN = "IsPublicDomain";

    @Autowired
    protected Warnings warnings;

    @Autowired
    private RealTimeMatchFetcher realTimeMatchFetcher;

    @Value("${scoringapi.propdata.shortcircuit:false}")
    protected boolean shouldShortcircuitPropdata;

    protected List<RealTimeMatchService> realTimeMatchServiceList;

    @Autowired
    protected MatchProxy matchProxy;

    @Autowired
    protected List<MatchInputBuilder> matchInputBuilders;

    @Autowired
    protected EnrichmentMetadataCache enrichmentMetadataCache;

    private ApplicationContext applicationContext;

    @PostConstruct
    public void initialize() throws Exception {
        if (shouldShortcircuitPropdata) {
            log.info("Initialize propdata fetcher executors as scoringapi-propdata shortcircuit is on.");
            Map<String, RealTimeMatchService> realTimeMatchServiceMap = //
                    applicationContext//
                            .getBeansOfType(RealTimeMatchService.class, //
                                    false, true);
            realTimeMatchServiceList = //
                    new ArrayList<RealTimeMatchService>(realTimeMatchServiceMap.values());
            realTimeMatchFetcher.initExecutors();
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

    protected RealTimeMatchService getRealTimeMatchService(String matchVersion) {
        for (RealTimeMatchService handler : realTimeMatchServiceList) {
            if (handler.accept(matchVersion)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { matchVersion });
    }

    protected List<LeadEnrichmentAttribute> getEnrichmentMetadata(CustomerSpace space) {
        List<LeadEnrichmentAttribute> selectedLeadEnrichmentAttributes = null;
        if (selectedLeadEnrichmentAttributes == null) {
            // we need to execute only once therefore null check is done
            selectedLeadEnrichmentAttributes = enrichmentMetadataCache.getEnrichmentAttributesMetadata(space);
        }
        return selectedLeadEnrichmentAttributes;
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
    
    private void mergeMatchedOutput(List<String> matchFieldNames, //
            OutputRecord outputRecord, //
            Map<String, FieldSchema> fieldSchemas, //
            Map<String, Object> record) {
        List<Object> matchFieldValues = outputRecord.getOutput();

        if (matchFieldNames.size() != matchFieldValues.size()) {
            throw new LedpException(LedpCode.LEDP_31005,
                    new String[] { String.valueOf(matchFieldNames.size()), String.valueOf(matchFieldValues.size()) });
        }

        for (int i = 0; i < matchFieldNames.size(); i++) {
            String fieldName = matchFieldNames.get(i);
            FieldSchema schema = fieldSchemas.get(fieldName);
            if (schema == null || (schema != null && schema.source != FieldSource.REQUEST)) {
                Object fieldValue = null;
                if (schema != null) {
                    fieldValue = FieldType.parse(schema.type, matchFieldValues.get(i));
                } else {
                    fieldValue = matchFieldValues.get(i);
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

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
