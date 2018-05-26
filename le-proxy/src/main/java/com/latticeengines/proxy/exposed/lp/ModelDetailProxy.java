package com.latticeengines.proxy.exposed.lp;

import com.latticeengines.domain.exposed.pls.ModelDetail;

public interface ModelDetailProxy {

    ModelDetail getModelDetail(String customerSpace, String modelId);

}
