package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.validator.routines.UrlValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.apps.cdl.service.VdbImportService;
import com.latticeengines.apps.cdl.workflow.ImportVdbTableAndPublishWorkflowSubmitter;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbGetLoadStatusConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableCancel;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("vdbImportService")
public class VdbImportServiceImpl implements VdbImportService {

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ImportVdbTableAndPublishWorkflowSubmitter importVdbTableAndPublishWorkflowSubmitter;

    private MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader =
            new MagicAuthenticationHeaderHttpRequestInterceptor();
    private List<ClientHttpRequestInterceptor> addMagicAuthHeaders =
            Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @PostConstruct
    private void setupRestTemplate() {
        restTemplate.setInterceptors(addMagicAuthHeaders);
    }

    @Override
    public String submitLoadTableJob(VdbLoadTableConfig loadConfig) {
        try {
            checkLoadConfig(loadConfig);
            return importVdbTableAndPublishWorkflowSubmitter.submit(loadConfig).toString();
        } catch (LedpException e) {
            VdbLoadTableStatus status = new VdbLoadTableStatus();
            status.setMessage(e.getMessage());
            status.setVdbQueryHandle(loadConfig.getVdbQueryHandle());
            switch (e.getCode()) {
                case LEDP_18136:
                case LEDP_18137:
                    status.setJobStatus("Running");
                    break;
                case LEDP_18138:
                    status.setJobStatus("Succeed");
                    break;
                default:
                    status.setJobStatus("Failed");
                    break;
            }
            restTemplate.postForEntity(loadConfig.getReportStatusEndpoint(), status, Void.class);
        }
        return "";
    }

    @Override
    public boolean cancelLoadTableJob(String applicationId, VdbLoadTableCancel cancelConfig) {
        VdbLoadTableStatus status = new VdbLoadTableStatus();
        status.setVdbQueryHandle(cancelConfig.getVdbQueryHandle());
        WorkflowExecutionId workflowExecutionId = workflowProxy.getWorkflowId(applicationId);
        boolean result = true;
        if (workflowExecutionId != null) {
            try {
                String customSpace = CustomerSpace.parse(cancelConfig.getTenantId()).toString();
                String collectionIdentifier = String.format("%s_%s_%s", customSpace, cancelConfig.getTableName(),
                        cancelConfig.getLaunchId());
                eaiJobDetailProxy.cancelImportJob(collectionIdentifier);
                status.setJobStatus("Aborted");
                status.setMessage(String.format("Application %s stopped", applicationId));
            } catch (Exception e) {
                result = false;
                status.setJobStatus("Failed");
                status.setMessage(String.format("Cancel job failed with exception: %s", e.toString()));
            }

        } else {
            result = false;
            status.setJobStatus("DoesNotExist");
            status.setMessage(String.format("Cannot find workflow for application id %s.", applicationId));
        }
        try {
            restTemplate.postForEntity(cancelConfig.getReportStatusEndpoint(), status, Void.class);
        } catch (RestClientException e) {

        }
        return result;
    }

    @Override
    public VdbLoadTableStatus getLoadTableStatus(VdbGetLoadStatusConfig config) {
        VdbLoadTableStatus vdbLoadTableStatus = new VdbLoadTableStatus();
        vdbLoadTableStatus.setVdbQueryHandle(config.getVdbQueryHandle());
        String customSpace = CustomerSpace.parse(config.getTenantId()).toString();
        String extractIdentifier = String.format("%s_%s_%s", customSpace, config.getTableName(), config.getLaunchId());
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetail(extractIdentifier);
        if (eaiImportJobDetail == null) {
            vdbLoadTableStatus.setJobStatus("DoesNotExist");
        } else {
            switch (eaiImportJobDetail.getStatus()) {
                case SUBMITTED:
                case RUNNING:
                    vdbLoadTableStatus.setJobStatus("Running");
                    break;
                case SUCCESS:
                    vdbLoadTableStatus.setJobStatus("Succeed");
                    break;
                case FAILED:
                    vdbLoadTableStatus.setJobStatus("Failed");
                    break;
            }
        }
        return vdbLoadTableStatus;
    }

    private void checkLoadConfig(VdbLoadTableConfig loadConfig) {
        if (StringUtils.isEmpty(loadConfig.getTenantId())) {
            throw new LedpException(LedpCode.LEDP_18133);
        }
        if (StringUtils.isEmpty(loadConfig.getVdbQueryHandle())) {
            throw new LedpException(LedpCode.LEDP_18134);
        }
        if (StringUtils.isEmpty(loadConfig.getTableName())) {
            throw new LedpException(LedpCode.LEDP_18129);
        }
        if (loadConfig.getTotalRows() <= 0) {
            throw new LedpException(LedpCode.LEDP_18130);
        }
        if (!validUrl(loadConfig.getGetQueryDataEndpoint())) {
            throw new LedpException(LedpCode.LEDP_18131);
        }
        if (!checkVdbSepcMetadata(loadConfig.getMetadataList())) {
            throw new LedpException(LedpCode.LEDP_18132);
        }
        if (VdbCreateTableRule.getCreateRule(loadConfig.getCreateTableRule()) == null) {
            throw new LedpException(LedpCode.LEDP_18135, new String[] {loadConfig.getCreateTableRule()});
        }
        if (tenantEntityMgr.findByTenantId(CustomerSpace.parse(loadConfig.getTenantId()).toString()) == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[] {loadConfig.getTenantId()});
        }
    }

    private boolean validUrl(String url) {
        String[] schemes = { "http", "https" };
        UrlValidator urlValidator = new UrlValidator(schemes, UrlValidator.ALLOW_LOCAL_URLS);
        return urlValidator.isValid(url);
    }

    private boolean checkVdbSepcMetadata(List<VdbSpecMetadata> metadataList) {
        for (VdbSpecMetadata metadata: metadataList) {
            if (StringUtils.isEmpty(metadata.getColumnName()) || StringUtils.isEmpty(metadata.getDataType())) {
                return false;
            }
        }
        return true;
    }
}
