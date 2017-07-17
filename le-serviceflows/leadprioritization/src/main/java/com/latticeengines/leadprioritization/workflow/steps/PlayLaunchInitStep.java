package com.latticeengines.leadprioritization.workflow.steps;

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.AccountProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStep.class);

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

    @Override
    public void execute() {
        Tenant tenant = null;
        PlayLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        try {
            log.info("Inside PlayLaunchInitStep execute()");
            tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info("For tenant: " + customerSpace.toString());
            log.info("For playId: " + playName);
            log.info("For playLaunchId: " + playLaunchId);

            PlayLaunch playLauch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, playName, playLaunchId);

            executeLaunchActivity(tenant, playLauch, config);

            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
        } catch (Exception ex) {
            internalResourceRestApiProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Failed);
        }

    }

    private void executeLaunchActivity(Tenant tenant, PlayLaunch playLauch, PlayLaunchInitStepConfiguration config) {
        // add processing logic

        // DUMMY LOGIC TO TEST INTEGRATION WITH recommendationService

        /*
        Restriction segmentRestrictionQuery = playLauch.getPlay().getSegment().getRestriction();
        int segmentAccountsCount = accountProxy.getSegmentAccountsCount(tenant.toString(), segmentRestrictionQuery);

        if (segmentAccountsCount > 0) {
            List<String> accountSchema = getSchema(TableRoleInCollection.BucketedAccount);
            DataRequest dataRequest = new DataRequest();
            dataRequest.setAttributes(accountSchema);

            int numberOfLoops = (int) Math.ceil((segmentAccountsCount * 1.0D) / pageSize);
            int alreadyReadAccounts = 0;

            for (int loopId = 0; loopId < numberOfLoops; loopId++) {
                int expectedPageSize = Math.min(pageSize, segmentAccountsCount - alreadyReadAccounts);
                DataPage accountPage = accountProxy.getSegmentAccounts(tenant.toString(), segmentRestrictionQuery,
                        alreadyReadAccounts, expectedPageSize, dataRequest);
                List<Map<String, Object>> accountList = accountPage.getData();
                alreadyReadAccounts += accountList.size();
        */
                for (int i = 0; i < 3; i++) {
                    Recommendation recommendation = createDummyRecommendation(tenant, playLauch, config);
                    recommendationService.create(recommendation);
                }
        /*
            }
        }
        */
    }

    private Recommendation createDummyRecommendation(Tenant tenant, PlayLaunch playLauch,
            PlayLaunchInitStepConfiguration config) {
        Random rand = new Random();
        String ACCOUNT_ID = "acc__" + System.currentTimeMillis() + rand.nextInt(50000);

        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLauch.getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);
        recommendation.setLaunchDate(playLauch.getCreatedTimestamp());
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(Math.min(0.5D, 1 / rand.nextDouble()));
        recommendation.setSynchronizationDestination("SFDC");
        recommendation.setPriorityDisplayName("A");

        return recommendation;
    }

    private List<String> getSchema(TableRoleInCollection role) {
        List<Attribute> schemaAttributes = getSchemaAttributes(role);

        Stream<String> stream = schemaAttributes.stream() //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId)) //
                .map(ColumnMetadata::getColumnId);

        return stream.collect(Collectors.toList());
    }

    private List<Attribute> getSchemaAttributes(TableRoleInCollection role) {
        String customerSpace = MultiTenantContext.getTenant().getId();
        Table schemaTable = dataCollectionProxy.getTable(customerSpace, role);
        List<Attribute> schemaAttributes = schemaTable.getAttributes();
        return schemaAttributes;
    }
}
