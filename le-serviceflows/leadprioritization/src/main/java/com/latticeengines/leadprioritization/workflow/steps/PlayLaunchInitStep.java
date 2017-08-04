package com.latticeengines.leadprioritization.workflow.steps;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${playmaker.workflow.segment.pagesize:100}")
    private int pageSize;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private RecommendationService recommendationService;

    @Autowired
    private AccountProxy accountProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private long launchTimestampMillis;

    @Override
    public void execute() {
        Tenant tenant = null;
        PlayLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        launchTimestampMillis = System.currentTimeMillis();

        try {
            log.info("Inside PlayLaunchInitStep execute()");
            tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info("For tenant: " + customerSpace.toString());
            log.info("For playId: " + playName);
            log.info("For playLaunchId: " + playLaunchId);

            PlayLaunch playLauch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
            Play play = internalResourceRestApiProxy.findPlayByName(customerSpace, playName);
            playLauch.setPlay(play);

            String segmentName = play.getSegmentName();
            log.info("Processing segment: " + segmentName);

            Restriction segmentRestrictionQuery = internalResourceRestApiProxy.getSegmentRestrictionQuery(customerSpace,
                    segmentName);

            log.info("Processing restriction: " + objectMapper.writeValueAsString(segmentRestrictionQuery));

            executeLaunchActivity(tenant, playLauch, config, segmentRestrictionQuery);

            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
        }

    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLauch, PlayLaunchInitStepConfiguration config,
            Restriction segmentRestriction) {
        // add processing logic

        // DUMMY LOGIC TO TEST INTEGRATION WITH recommendationService

        long segmentAccountsCount = accountProxy.getAccountsCount(tenant.getId(), segmentRestriction);
        log.info("Total records in segment: " + segmentAccountsCount);

        if (segmentAccountsCount > 0) {
            List<String> accountSchema = getSchema(tenant, TableRoleInCollection.BucketedAccount);
            DataRequest dataRequest = new DataRequest();
            dataRequest.setAttributes(accountSchema);

            int numberOfLoops = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);

            log.info("Number of required loops: " + numberOfLoops + ", with pageSize: " + pageSize);

            String[] fields = new String[] { PlaymakerConstants.CdlKeyZip, //
                    PlaymakerConstants.AccountId, //
                    PlaymakerConstants.CdlKeyExternal_ID, //
                    PlaymakerConstants.SalesforceAccountID, //
                    PlaymakerConstants.CdlKeyTotalMonetaryValue, //
                    PlaymakerConstants.CdlKeyDisplayName, //
                    PlaymakerConstants.CdlKeyCrmAccount_External_ID };

            long alreadyReadAccounts = 0;

            for (int loopId = 0; loopId < numberOfLoops; loopId++) {
                log.info("Loop #" + loopId);

                int expectedPageSize = (int) Math.min(pageSize * 1L, (segmentAccountsCount - alreadyReadAccounts));
                DataPage accountPage = accountProxy.getAccounts(tenant.getId(), segmentRestriction,
                        (int) alreadyReadAccounts, expectedPageSize, fields);

                log.info("Got #" + accountPage.getData().size() + " elements in this loop");

                List<Map<String, Object>> accountList = accountPage.getData();
                alreadyReadAccounts += accountList.size();

                for (Map<String, Object> account : accountList) {
                    // Recommendation recommendation =
                    // createDummyRecommendation(tenant, playLauch, config);

                    Recommendation recommendation = createRecommendation(tenant, playLauch, config, account);
                    recommendationService.create(recommendation);
                }

            }
        }

    }

    private Recommendation createRecommendation(Tenant tenant, PlayLaunch playLauch,
            PlayLaunchInitStepConfiguration config, Map<String, Object> account) {
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLauch.getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLauch.getCreatedTimestamp();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(account.get(PlaymakerConstants.AccountId).toString());
        recommendation.setLeAccountExternalID(account.get(PlaymakerConstants.CdlKeyExternal_ID).toString());
        recommendation.setSfdcAccountID(account.get(PlaymakerConstants.SalesforceAccountID).toString());

        String valueStr = account.get(PlaymakerConstants.CdlKeyTotalMonetaryValue).toString();
        Double value = 0D;
        if (StringUtils.isNotEmpty(valueStr) && StringUtils.isNumeric(valueStr)) {
            value = Double.parseDouble(valueStr);
        }
        recommendation.setMonetaryValue(value);
        recommendation.setCompanyName(account.get(PlaymakerConstants.CdlKeyDisplayName).toString());
        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(0.5D);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);
        recommendation.setPriorityDisplayName("A");

        return recommendation;
    }

    private List<String> getSchema(Tenant tenant, TableRoleInCollection role) {
        List<Attribute> schemaAttributes = getSchemaAttributes(tenant, role);

        Stream<String> stream = schemaAttributes.stream() //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId)) //
                .map(ColumnMetadata::getColumnId);

        return stream.collect(Collectors.toList());
    }

    private List<Attribute> getSchemaAttributes(Tenant tenant, TableRoleInCollection role) {
        String customerSpace = tenant.getId();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }
}
