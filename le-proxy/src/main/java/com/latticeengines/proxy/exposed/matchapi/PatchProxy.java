package com.latticeengines.proxy.exposed.matchapi;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
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
}
