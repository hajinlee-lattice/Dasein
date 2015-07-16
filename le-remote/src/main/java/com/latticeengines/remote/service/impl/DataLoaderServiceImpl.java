package com.latticeengines.remote.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.vdb.SpecParser;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.dataloader.GetSpecRequest;
import com.latticeengines.domain.exposed.dataloader.GetSpecResult;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.remote.exposed.service.Headers;
import com.latticeengines.remote.util.CrmUtils;
import com.latticeengines.remote.util.DlConfigUtils;

@Component("dataLoaderService")
public class DataLoaderServiceImpl implements DataLoaderService {

    @VisibleForTesting
    static final String DEFAULT_SEGMENT = "LATTICE_DEFAULT_SEGMENT";

    private static final Log log = LogFactory.getLog(DataLoaderServiceImpl.class);

    private static final String DL_REST_SERVICE = "/DLRestService";
    private static final String GET_SPEC_DETAILS = "/GetSpecDetails";
    private static final String INSTALL_VISIDB_STRUC_SYNC = "/InstallVisiDBStructureFile_Sync";
    private static final String INSTALL_CONFIG_SYNC = "/InstallConfigFile_Sync";
    private static final String GET_TENANT_SETTINGS = "/GetDLTenantSettings";
    private static final String CREATE_TENANT = "/CreateDLTenant";
    private static final String DELETE_TENANT = "/DeleteDLTenant";
    private static final String VALIDATE_CREDS = "/ValidateExternalAPICredentials";
    private static final String UPDATE_DATA_PROVIDER = "/UpdateDataProvider";
    private static final String DOWNLOAD_CONFIG = "/DownloadConfigFile";

    private static final String SEGMENT_PREFIX = "DefnSegment_";
    private static final String MODELID_PREFIX = "LatticeFunctionExpressionConstant(\"";
    private static final String LATTICE_FUNCTION_EXPRESSION_CONSTANT_SCALAR = "LatticeFunctionExpressionConstantScalar(\"";
    private static final String SEGMENTS_SPEC = "AppData_Segments";
    private static final String VERSION_SPEC = "Version";
    private static final String SEGMENT_MODELS_SPEC = "AppData_Model_GUID";
    private static final String SPEC_PREFIX = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace name=\"Workspace\">\n      <specs>\nSpecLatticeNamedElements((        \nSpecLatticeNamedElement(\n  SpecLatticeFunction(";
    private static final String SPEC_POSTFIX = "   ,DataTypeUnknown\n   ,SpecFunctionTypeMetric\n   ,SpecFunctionSourceTypeCalculation\n   ,SpecDefaultValueNull\n   ,SpecDescription(\"\")\n  )\n,ContainerElementName(\"AppData_Model_GUID\")\n)\n))\n      </specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>";
    private static final String SEGMENT_TEMPLATE = "    LatticeFunctionExpression(\n      LatticeFunctionOperatorIdentifier(\"IF\")\n     ,LatticeFunctionIdentifier(ContainerElementName(\"DefnSegment_{SEGMENT_NAME}\"))\n     ,LatticeFunctionExpressionConstant(\"{MODELID}\",DataTypeVarChar({MODELID_LENGTH}))";
    private static final String DEFAULT_SEGMENT_TEMPLATE = "LatticeFunctionExpressionConstant(\"{MODELID}\",DataTypeVarChar({MODELID_LENGTH}))";
    private static final String MODELID_TOKEN = "{MODELID}";
    private static final String MODELIDLENGTH_TOKEN = "{MODELID_LENGTH}";
    private static final String SEGMENTNAME_TOKEN = "{SEGMENT_NAME}";
    private static final String UNINITIALIZED_MODELID = "";

    private static final int STATUS_SUCCESS = 3;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<String> getSegmentNames(String tenantName, String dlUrl) {
        List<String> result = Collections.emptyList();
        String jsonStr = JsonUtils.serialize(new GetSpecRequest(tenantName, SEGMENTS_SPEC));
        String response;
        try {
            response = callDLRestService(dlUrl, GET_SPEC_DETAILS, jsonStr);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21002, ex);
        }

        GetSpecResult getSpecResult = JsonUtils.deserialize(response, GetSpecResult.class);

        if (getSpecResult.getSuccess().equalsIgnoreCase("true")) {
            String details = getSpecResult.getSpecDetails();
            int start = details.indexOf(LATTICE_FUNCTION_EXPRESSION_CONSTANT_SCALAR)
                    + LATTICE_FUNCTION_EXPRESSION_CONSTANT_SCALAR.length();
            int end = details.indexOf("\"", start);

            if (end > start) {
                String segmentStr = details.substring(start, end);
                String[] segments = segmentStr.split(";");
                result = Arrays.asList(segments);
            }
        }

        return result;
    }

    @Override
    public List<Segment> getSegments(String tenantName, String dlUrl) {
        List<Segment> finalResults = new ArrayList<>();
        LinkedHashMap<String, Segment> nameToSegments = getNameToSegmentMap(tenantName, dlUrl);

        LinkedHashSet<String> segmentNames = new LinkedHashSet<>(getSegmentNames(tenantName, dlUrl));
        List<String> notYetPrioritizedSegmentNames = new ArrayList<>();
        for (String name : segmentNames) {
            if (!nameToSegments.containsKey(name)) {
                notYetPrioritizedSegmentNames.add(name);
            }
        }

        int priority = 1;
        for (String name : nameToSegments.keySet()) {
            if (segmentNames.contains(name)) {
                Segment matchedSegment = nameToSegments.get(name);
                matchedSegment.setPriority(priority);
                finalResults.add(matchedSegment);
            }
            priority++;
        }

        for (String name : notYetPrioritizedSegmentNames) {
            Segment segment = new Segment();
            segment.setName(name);
            segment.setModelId("");
            segment.setPriority(priority++);
            finalResults.add(segment);
        }

        Segment defaultSegment = nameToSegments.get(DEFAULT_SEGMENT);
        defaultSegment.setPriority(priority);
        finalResults.add(defaultSegment);

        return finalResults;
    }

    private LinkedHashMap<String, Segment> getNameToSegmentMap(String tenantName, String dlUrl) {
        LinkedHashMap<String, Segment> nameToSegments = new LinkedHashMap<>();

        String jsonStr = JsonUtils.serialize(new GetSpecRequest(tenantName, SEGMENT_MODELS_SPEC));
        String response;
        try {
            response = callDLRestService(dlUrl, GET_SPEC_DETAILS, jsonStr);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21002, ex);
        }

        GetSpecResult getSpecResult = JsonUtils.deserialize(response, GetSpecResult.class);
        if ("true".equalsIgnoreCase(getSpecResult.getSuccess())) {
            nameToSegments = parseSegmentSpec(getSpecResult.getSpecDetails());
        } else if ("false".equalsIgnoreCase(getSpecResult.getSuccess())
                && getSpecResult.getErrorMessage().contains("does not exist")) {
            // spec does not exist, instantiate with default segment
            Segment segment = new Segment();
            segment.setName(DEFAULT_SEGMENT);
            segment.setModelId("");
            segment.setPriority(1);
            nameToSegments.put(DEFAULT_SEGMENT, segment);
        }

        return nameToSegments;
    }

    @VisibleForTesting
    LinkedHashMap<String, Segment> parseSegmentSpec(String details) {
        LinkedHashMap<String, Segment> results = new LinkedHashMap<>();

        int index = 0;
        int priority = 1;
        boolean defaultFound = false;

        while (!defaultFound) {
            String modelId;
            Segment segment = new Segment();
            int newIndex = details.indexOf(SEGMENT_PREFIX, index);
            if (newIndex != -1) {
                Extract segmentExtract = extractValue(SEGMENT_PREFIX, details, index);
                segment.setName(segmentExtract.value);
                Extract modelIdExtract = extractValue(MODELID_PREFIX, details, segmentExtract.postIndex);
                modelId = modelIdExtract.value;
                index = modelIdExtract.postIndex;
            } else {
                // handle special "default" segment case
                segment.setName(DEFAULT_SEGMENT);
                modelId = extractValue(MODELID_PREFIX, details, index).value;
                defaultFound = true;
            }
            segment.setModelId(modelId);
            segment.setPriority(priority++);
            results.put(segment.getName(), segment);
        }

        return results;
    }

    private Extract extractValue(String searchString, String targetString, int startIndex) {
        int index = targetString.indexOf(searchString, startIndex);
        Extract extract = null;

        if (index != -1) {
            int start = index + searchString.length();
            int end = targetString.indexOf("\"", start);
            if (end > start) {
                extract = new Extract(targetString.substring(start, end), end);
            } else if (end == start) {
                extract = new Extract("", end);
            }
        }

        if (index == -1 || extract == null) {
            throw new LedpException(LedpCode.LEDP_21000, new String[] { targetString });
        }

        return extract;
    }

    @Override
    public InstallResult setSegments(String tenantName, String dlUrl, List<Segment> segments) {
        // There is a potential for data loss if user is viewing and modifying
        // segments while someone has added new segments on the backend.
        // Therefore, first perform reconciliation; if current set of segments
        // differs from updated set of segments then raise exception.
        List<String> currentSegments = getSegmentNames(tenantName, dlUrl);
        if (!compareSegments(currentSegments, segments)) {
            throw new LedpException(LedpCode.LEDP_21003,
                    new String[] { segments.toString(), currentSegments.toString() });
        }

        StringBuilder spec = new StringBuilder();
        spec.append(SPEC_PREFIX);

        if (segments.size() == 1) {
            Segment segment = segments.get(0);
            spec.append(marshallDefaultSegmentSpec(segment));
        } else if (segments.size() > 1) {
            Collections.sort(segments, new SegmentComparator());
            Segment defaultSegment = null;
            for (Segment segment : segments) {
                if (segment.getName().equals(DEFAULT_SEGMENT)) {
                    defaultSegment = segment;
                    continue;
                }
                String templateA = SEGMENT_TEMPLATE.replace(SEGMENTNAME_TOKEN, segment.getName());
                String templateB = templateA.replace(MODELID_TOKEN, getActualOrUninitializedModelId(segment));
                String templateC = templateB.replace(MODELIDLENGTH_TOKEN,
                        String.valueOf(getActualOrUninitializedModelId(segment).length()));

                spec.append(templateC).append(",");
            }
            spec.append(marshallDefaultSegmentSpec(defaultSegment));
            for (int i = 0; i < segments.size() - 1; i++) {
                spec.append(")");
            }

        }

        spec.append(SPEC_POSTFIX);

        InstallTemplateRequest request = new InstallTemplateRequest(tenantName, spec.toString());
        InstallResult result = installVisiDBStructureFile(request, dlUrl);
        if (result.getStatus() != 3 && result.getErrorMessage() != null) {
            throw new LedpException(LedpCode.LEDP_21001, new String[] { String.valueOf(result.getStatus()),
                    result.getErrorMessage() });
        }
        return result;
    }

    @VisibleForTesting
    boolean compareSegments(List<String> currentSegments, List<Segment> newSegments) {
        Set<String> currentSet = new HashSet<>(currentSegments);
        Set<String> newSet = new HashSet<>();

        for (Segment segment : newSegments) {
            if (!segment.getName().equals(DEFAULT_SEGMENT)) {
                newSet.add(segment.getName());
            }
        }
        return currentSet.equals(newSet);
    }

    private class SegmentComparator implements Comparator<Segment> {
        @Override
        public int compare(Segment o1, Segment o2) {
            return o1.getPriority().compareTo(o2.getPriority());
        }
    }

    private String marshallDefaultSegmentSpec(Segment segment) {
        String templateA = DEFAULT_SEGMENT_TEMPLATE.replace(MODELID_TOKEN, getActualOrUninitializedModelId(segment));
        return templateA
                .replace(MODELIDLENGTH_TOKEN, String.valueOf(getActualOrUninitializedModelId(segment).length()));
    }

    private String getActualOrUninitializedModelId(Segment segment) {
        return Strings.isNullOrEmpty(segment.getModelId()) ? UNINITIALIZED_MODELID : segment.getModelId();
    }

    @Override
    public InstallResult installVisiDBStructureFile(InstallTemplateRequest request, String dlUrl) {
        try {
            String response = callDLRestService(dlUrl, INSTALL_VISIDB_STRUC_SYNC, request);
            return JsonUtils.deserialize(response, InstallResult.class);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21002, ex);
        }
    }

    @Override
    public InstallResult installDataLoaderConfigFile(InstallTemplateRequest request, String dlUrl) {
        try {
            String response = callDLRestService(dlUrl, INSTALL_CONFIG_SYNC, request);
            return JsonUtils.deserialize(response, InstallResult.class);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21004, ex);
        }

    }

    @Override
    public InstallResult getDLTenantSettings(GetVisiDBDLRequest getRequest, String dlUrl) {
        try {
            String response = callDLRestService(dlUrl, GET_TENANT_SETTINGS, getRequest);
            return JsonUtils.deserialize(response, InstallResult.class);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21005, ex);
        }
    }

    @Override
    public InstallResult deleteDLTenant(DeleteVisiDBDLRequest request, String dlUrl, boolean retry) {
        try{
            String response = callDLRestService(dlUrl, DELETE_TENANT, request);
            return JsonUtils.deserialize(response, InstallResult.class);
        }catch (IOException ex) {
               throw new LedpException(LedpCode.LEDP_21007, ex);
        }
    }

    @Override
    public InstallResult createDLTenant(CreateVisiDBDLRequest postRequest, String dlUrl) {
        try {
            String response = callDLRestService(dlUrl, CREATE_TENANT, postRequest);
            return JsonUtils.deserialize(response, InstallResult.class);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21006, ex);
        }
    }

    private class Extract {
        private String value;
        private int postIndex;

        Extract(String value, int postIndex) {
            super();
            this.value = value;
            this.postIndex = postIndex;
        }
    }

    @Override
    public String getTemplateVersion(String tenantName, String dlUrl) {
        try {
            String response = callDLRestService(dlUrl, GET_SPEC_DETAILS, new GetSpecRequest(tenantName, VERSION_SPEC));
            GetSpecResult getSpecResult = JsonUtils.deserialize(response, GetSpecResult.class);
            if (!"true".equalsIgnoreCase(getSpecResult.getSuccess())) {
                log.warn("Can not get template version! error=" + getSpecResult.getErrorMessage());
                return "";
            }
            SpecParser sp = new SpecParser(getSpecResult.getSpecDetails());
            return sp.getTemplate();

        } catch (Exception ex) {
            log.warn("Can not get template version! tenantName=" + tenantName + " dlUrl=" + dlUrl, ex);
            return "";
        }
    }

    @Override
    public void verifyCredentials(String crmType, CrmCredential crmCredential, boolean isProduction, String dlUrl) {
        try {
            Map<String, String> paramerters = CrmUtils.verificationParameters(crmType, crmCredential, isProduction);
            String response = callDLRestService(dlUrl, VALIDATE_CREDS, paramerters);
            if (!CrmUtils.checkVerificationStatus(response)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jNode = mapper.readTree(response);
                for (JsonNode node: jNode.get("Value")) {
                    if (node.get("Key").asText().equals("Info")) {
                        throw new RuntimeException(String.format("CRM verification failed: %s",
                                node.get("Value").asText()));
                    }
                }
                throw new RuntimeException("CRM verification failed for an unknonw reason.");
            }
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    @Override
    public void updateDataProvider(String crmType, String plsTenantId, CrmConfig crmConfig, String dlUrl) {
        if (crmType.equals(CrmUtils.CRM_ELOQUA)) {
            crmConfig.setDataProviderName("Eloqua_DataProvider");
            Map<String, Object> parameters = CrmUtils.dataProviderParameters(crmType, plsTenantId, crmConfig);
            executeUpdateDataProviderRequest(dlUrl, parameters);

            crmConfig.setDataProviderName("Eloqua_Bulk_DataProvider");
            parameters = CrmUtils.dataProviderParameters(CrmUtils.CRM_ELOQUA_BULK, plsTenantId, crmConfig);
            executeUpdateDataProviderRequest(dlUrl, parameters);
            return;
        }

        if (crmType.equals(CrmUtils.CRM_MARKETO)) {
            crmConfig.setDataProviderName("Marketo_DataProvider");
            Map<String, Object> parameters = CrmUtils.dataProviderParameters(crmType, plsTenantId, crmConfig);
            executeUpdateDataProviderRequest(dlUrl, parameters);
        }

        if (crmType.equals(CrmUtils.CRM_SFDC)) {
            crmConfig.setDataProviderName("SFDC_DataProvider");
            Map<String, Object> parameters = CrmUtils.dataProviderParameters(crmType, plsTenantId, crmConfig);
            executeUpdateDataProviderRequest(dlUrl, parameters);
        }
    }

    @Override
    public String getSfdcUser(String tenantName, String dlUrl) {
        String config = getDLConfig(tenantName, dlUrl);
        return DlConfigUtils.parseSfdcUser(config);
    }

    @Override
    public String getMarketoUserId(String tenantName, String dlUrl) {
        String config = getDLConfig(tenantName, dlUrl);
        return DlConfigUtils.parseMarketoUserId(config);
    }

    @Override
    public String getMarketoUrl(String tenantName, String dlUrl) {
        String config = getDLConfig(tenantName, dlUrl);
        return DlConfigUtils.parseMarketoUrl(config);
    }

    @Override
    public String getEloquaUsername(String tenantName, String dlUrl) {
        String config = getDLConfig(tenantName, dlUrl);
        return DlConfigUtils.parseEloquaUsername(config);
    }

    @Override
    public String getEloquaCompany(String tenantName, String dlUrl) {
        String config = getDLConfig(tenantName, dlUrl);
        return DlConfigUtils.parseEloquaCompany(config);
    }

    private String getDLConfig(String tenantName, String dlUrl) {
        Map<String, String> paramerters = new HashMap<>();
        paramerters.put("tenantName", tenantName);
        String response;
        try {
            response = callDLRestService(dlUrl, DOWNLOAD_CONFIG, paramerters);
            JsonNode json = objectMapper.readTree(response);
            if (json.get("Status").asInt() != STATUS_SUCCESS) {
                throw new IllegalStateException("Returned status from DL is not SUCCESS.");
            }
            for(JsonNode kvpair: json.get("Value")) {
                if("Config".equalsIgnoreCase(kvpair.get("Key").asText())) {
                    return kvpair.get("Value").asText();
                }
            }
            throw new IOException("Cannot find Config file in the response.");
        } catch (IOException|IllegalStateException ex) {
            throw new LedpException(LedpCode.LEDP_21002, ex);
        }
    }

    private void executeUpdateDataProviderRequest(String dlUrl, Map<String, Object> parameters) {
        try {
            String response = callDLRestService(dlUrl, UPDATE_DATA_PROVIDER, parameters);
            CrmUtils.checkUpdateDataProviderStatus(response);
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18035, ex, new String[] { ex.getMessage() });
        }
    }

    private String callDLRestService(String dlUrl, String endpoint, Object payload) throws IOException {
        if (dlUrl.endsWith("/")) dlUrl = dlUrl.substring(0, dlUrl.length() - 1);
        if (!dlUrl.endsWith(DL_REST_SERVICE)) dlUrl += DL_REST_SERVICE;

        String stringifiedPayload;
        if (payload instanceof String) {
            stringifiedPayload = ( String ) payload;
        } else {
            stringifiedPayload =  JsonUtils.serialize(payload);
        }

        log.info("Send POST to " + dlUrl + endpoint + " with payload = " + stringifiedPayload);

        return HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + endpoint, false,
                Headers.getHeaders(), stringifiedPayload);

    }
}
