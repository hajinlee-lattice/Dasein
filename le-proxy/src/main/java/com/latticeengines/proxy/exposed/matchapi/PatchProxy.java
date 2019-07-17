package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchValidationResponse;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("patchProxy")
public class PatchProxy extends BaseRestApiProxy {
    public PatchProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/patches");
    }

    public PatchValidationResponse validatePatchBook(PatchBook.Type patchBookType, PatchRequest request) {
        String url = constructUrl("/validate/{patchBookType}", patchBookType.name());
        return postKryo("validate_patch", url, request, PatchValidationResponse.class);
    }

    public PatchValidationResponse validatePatchBookWithPidPagination(PatchBook.Type patchBookType,
            PatchRequest request) {
        String url = constructUrl("/validatePagination/{patchBookType}", patchBookType.name());
        return postKryo("validate_patch", url, request, PatchValidationResponse.class);
    }

    public PatchValidationResponse findPatchBookMinMaxPid(PatchBook.Type patchBookType, String dataCloudVersion,
            PatchMode mode) {
        String url = constructUrl("/findMinMaxPid/{patchBookType}", patchBookType.name());
        String queryStr = String.format("?dataCloudVersion=%s&mode=%s", dataCloudVersion, mode.name());
        return getKryo("find_patchbook_min_max_pid", url + queryStr, PatchValidationResponse.class);
    }
}
