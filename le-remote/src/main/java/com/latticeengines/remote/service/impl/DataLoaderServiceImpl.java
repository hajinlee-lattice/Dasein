package com.latticeengines.remote.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

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
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.remote.exposed.service.Headers;

@Component("dataLoaderService")
public class DataLoaderServiceImpl implements DataLoaderService {

    private static final String GET_SPEC_DETAILS = "/GetSpecDetails";

    @VisibleForTesting
    static final String DEFAULT_SEGMENT = "LATTICE_DEFAULT_SEGMENT";

    private static final Log log = LogFactory.getLog(DataLoaderServiceImpl.class);

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

    public List<String> getSegmentNames(String tenantName, String dlUrl) {
        List<String> result = Collections.emptyList();
        String jsonStr = JsonUtils.serialize(new GetSpecRequest(tenantName, SEGMENTS_SPEC));
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + GET_SPEC_DETAILS, false,
                    Headers.getHeaders(), jsonStr);
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
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + GET_SPEC_DETAILS, false,
                    Headers.getHeaders(), jsonStr);
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
    LinkedHashMap<String,Segment> parseSegmentSpec(String details) {
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

    public InstallResult installVisiDBStructureFile(InstallTemplateRequest request, String dlUrl) {
        String jsonStr = JsonUtils.serialize(request);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/InstallVisiDBStructureFile_Sync",
                    false, Headers.getHeaders(), jsonStr);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21002, ex);
        }
        return JsonUtils.deserialize(response, InstallResult.class);
    }

    public InstallResult installDataLoaderConfigFile(InstallTemplateRequest request, String dlUrl) {
        String jsonStr = JsonUtils.serialize(request);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(
                    dlUrl + "/DLRestService/InstallConfigFile_Sync", false, Headers.getHeaders(), jsonStr);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21004, ex);
        }
        return JsonUtils.deserialize(response, InstallResult.class);
    }

    public InstallResult getDLTenantSettings(GetVisiDBDLRequest getRequest, String dlUrl) {
        String jsonString = JsonUtils.serialize(getRequest);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/GetDLTenantSettings",
                    true, Headers.getHeaders(), jsonString);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21005, ex);
        }
        return JsonUtils.deserialize(response, InstallResult.class);
    }

    public InstallResult deleteDLTenant(DeleteVisiDBDLRequest request, String dlUrl, boolean retry) {
        String jsonStr = JsonUtils.serialize(request);
        String response = "";
        try{ 
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/DeleteDLTenant",
                retry, Headers.getHeaders(), jsonStr);
        }catch (IOException ex) {
               throw new LedpException(LedpCode.LEDP_21007, ex);
        }
        return JsonUtils.deserialize(response, InstallResult.class);
    }

    public InstallResult createDLTenant(CreateVisiDBDLRequest postRequest, String dlUrl) {
        String jsonString = JsonUtils.serialize(postRequest);
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + "/DLRestService/CreateDLTenant", false,
                    Headers.getHeaders(), jsonString);
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_21006, ex);
        }
        return JsonUtils.deserialize(response, InstallResult.class);
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
        String jsonStr = JsonUtils.serialize(new GetSpecRequest(tenantName, VERSION_SPEC));
        String response = "";
        try {
            response = HttpClientWithOptionalRetryUtils.sendPostRequest(dlUrl + GET_SPEC_DETAILS, false,
                    Headers.getHeaders(), jsonStr);
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
}
