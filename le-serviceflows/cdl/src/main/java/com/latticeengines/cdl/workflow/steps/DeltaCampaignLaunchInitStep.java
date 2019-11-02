package com.latticeengines.cdl.workflow.steps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.export.BaseSparkSQLStep;
import com.latticeengines.cdl.workflow.steps.play.CampaignLaunchProcessor;
import com.latticeengines.cdl.workflow.steps.play.PlayLaunchContext;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchInitStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.spark.exposed.service.SparkJobService;

@Component("deltaCampaignLaunchInitStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchInitStep extends BaseSparkSQLStep<DeltaCampaignLaunchInitStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchInitStep.class);

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CampaignLaunchProcessor campaignLaunchProcessor;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    protected SparkJobService sparkJobService;

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Override
    public void execute() {
        DeltaCampaignLaunchInitStepConfiguration config = getConfiguration();
        CustomerSpace customerSpace = config.getCustomerSpace();
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();

        try {
            log.info("Inside CampaignLaunchInitStep execute()");
            Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace.toString());

            log.info(String.format("For tenant: %s", customerSpace.toString()) + "\n"
                    + String.format("For playId: %s", playName) + "\n"
                    + String.format("For playLaunchId: %s", playLaunchId));

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18157, ex);
        }
    }

    private void successUpdates(CustomerSpace customerSpace, String playName, String playLaunchId) {
        playProxy.updatePlayLaunch(customerSpace.toString(), playName, playLaunchId, LaunchState.Launched);
        playProxy.publishTalkingPoints(customerSpace.toString(), playName);
    }

    private void setCustomDisplayNames(PlayLaunchContext playLaunchContext) {
        List<ColumnMetadata> columnMetadata = playLaunchContext.getFieldMappingMetadata();
        if (CollectionUtils.isNotEmpty(columnMetadata)) {
            Map<String, String> contactDisplayNames = columnMetadata.stream()
                    .filter(col -> BusinessEntity.Contact.equals(col.getEntity()))
                    .collect(Collectors.toMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName));
            Map<String, String> accountDisplayNames = columnMetadata.stream()
                    .filter(col -> !BusinessEntity.Contact.equals(col.getEntity()))
                    .collect(Collectors.toMap(ColumnMetadata::getAttrName, ColumnMetadata::getDisplayName));
            log.info("accountDisplayNames map: " + accountDisplayNames);
            log.info("contactDisplayNames map: " + contactDisplayNames);

            putObjectInContext(RECOMMENDATION_ACCOUNT_DISPLAY_NAMES, accountDisplayNames);
            putObjectInContext(RECOMMENDATION_CONTACT_DISPLAY_NAMES, contactDisplayNames);
        }
    }

    @Override
    protected CustomerSpace parseCustomerSpace(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Version parseDataCollectionVersion(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected String parseEvaluationDateStr(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected AttributeRepository parseAttrRepo(DeltaCampaignLaunchInitStepConfiguration stepConfiguration) {
        // TODO Auto-generated method stub
        return null;
    }

}
