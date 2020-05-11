package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.dcp.UpdateSourceRequest;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsRequest;
import com.latticeengines.domain.exposed.pls.frontend.ValidateFieldDefinitionsResponse;

@Component("testSourceProxy")
public class TestSourceProxy extends PlsRestApiProxyBase {

    public TestSourceProxy() {
        super("pls/sources");
    }

    public Source createSource(SourceRequest sourceRequest) {
        String url = constructUrl("/");
        return post("create source", url, sourceRequest, Source.class);
    }

    public Source getSource(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        return get("get source", url, Source.class);
    }

    public List<Source> getSourcesByProject(String projectId) {
        String url = constructUrl("/projectId/{projectId}", projectId);
        List<?> rawList = get("get source list", url, List.class);
        return JsonUtils.convertList(rawList, Source.class);
    }

    public void deleteSourceById(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        delete("delete source", url);
    }

    public void pauseSourceById(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}/pause", sourceId);
        put("pause source", url);
    }

    public FetchFieldDefinitionsResponse fetchDefinitions(String sourceId, String entityType,
                                                          String importFile) {
        String url = constructUrl("/fetch/?importFile={importFile}", importFile);
        if (StringUtils.isNotBlank(sourceId)) {
            url += "&sourceId=" + sourceId;
        }
        if (StringUtils.isNotBlank(entityType)) {
            url += "&entityType=" + entityType;
        }
        return get("get definitions", url, FetchFieldDefinitionsResponse.class);
    }

    public ValidateFieldDefinitionsResponse validateFieldDefinitions(String importFile,
                                                                     ValidateFieldDefinitionsRequest validateRequest) {
        String url = constructUrl("/validate/?importFile={importFile}", importFile);
        return post("validate definitions", url, validateRequest, ValidateFieldDefinitionsResponse.class);
    }

    public FieldDefinitionsRecord getSourceMappings(String sourceId) {
        String url = constructUrl("/sourceId/{sourceId}/mappings", sourceId);
        return get("get source mappings", url, FieldDefinitionsRecord.class);
    }

    public Source updateSource(String sourceId, UpdateSourceRequest updateSourceRequest) {
        String url = constructUrl("/sourceId/{sourceId}", sourceId);
        return put("update source", url, updateSourceRequest, Source.class);
    }
}
