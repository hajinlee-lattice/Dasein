
package com.latticeengines.serviceflows.workflow.export;

import static com.latticeengines.domain.exposed.admin.LatticeFeatureFlag.PUBLISH_TO_ELASTICSEARCH;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.elasticsearch.PublishTableToESRequest;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.PublishToElasticSearchConfiguration;
import com.latticeengines.proxy.exposed.cdl.PublishTableProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("publishToElasticSearch")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PublishToElasticSearch extends BaseWorkflowStep<PublishToElasticSearchConfiguration> {

    private static Logger log = LoggerFactory.getLogger(PublishToElasticSearch.class);

    @Inject
    private PublishTableProxy publishTableProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private RestHighLevelClient client;

    @Value("${elasticsearch.shards}")
    private int esShards;

    @Value("${elasticsearch.replicas}")
    private int esReplicas;

    @Value("${elasticsearch.refreshinterval}")
    private String esRefreshInterval;

    @Value("${elasticsearch.dynamic}")
    private boolean esDynamic;

    @Value("${elasticsearch.http.scheme}")
    private String esHttpScheme;

    @Value("${elasticsearch.host}")
    private String esHost;

    @Value("${elasticsearch.ports}")
    private String esPorts;

    @Value("${elasticsearch.user}")
    private String user;

    @Value("${elasticsearch.pwd.encrypted}")
    private String password;

    @Override
    public void execute() {
        if (!elasticSearchEnabled()) {
            log.info("skip this step as feature flag is not enabled in {}", configuration.getCustomerSpace());
            return ;
        }

        List<ElasticSearchExportConfig> configs = getExportConfigs();
        if (CollectionUtils.isEmpty(configs)) {
            log.info("No tables need to populate, skip execution.");
            return;
        }

        PublishTableToESRequest request = new PublishTableToESRequest();
        request.setExportConfigs(configs);
        request.setEsConfig(generateCDLElasticSearchConfig());

        String appId = publishTableProxy.publishTableToES(configuration.getCustomerSpace().toString(), request);
        log.info("appId is {}", appId);
    }


    private List<ElasticSearchExportConfig> getExportConfigs() {
        return  getListObjectFromContext(TABLES_GOING_TO_ES, ElasticSearchExportConfig.class);
    }

    public ElasticSearchConfig generateCDLElasticSearchConfig() {
        ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig();
        elasticSearchConfig.setShards(esShards);
        elasticSearchConfig.setReplicas(esReplicas);
        elasticSearchConfig.setRefreshInterval(esRefreshInterval);
        elasticSearchConfig.setDynamic(esDynamic);
        elasticSearchConfig.setEsHost(esHost);
        elasticSearchConfig.setEsPort(esPorts);
        elasticSearchConfig.setEsUser(user);
        String encryptionKey = CipherUtils.generateKey();
        elasticSearchConfig.setEncryptionKey(encryptionKey);
        String saltHint = CipherUtils.generateKey();
        elasticSearchConfig.setSalt(saltHint);
        elasticSearchConfig.setEsPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));
        log.info("password is {}, encrypted password {}", password, elasticSearchConfig.getEsPassword());
        elasticSearchConfig.setHttpScheme(esHttpScheme);
        return elasticSearchConfig;
    }

    private boolean elasticSearchEnabled() {
        return batonService.isEnabled(configuration.getCustomerSpace(), PUBLISH_TO_ELASTICSEARCH);
    }
}
