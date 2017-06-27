package com.latticeengines.pls.proxy;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;

@Component("testDataCollectionProxy")
public class TestDataFeedProxy extends ProtectedRestApiProxy {

    public TestDataFeedProxy() {
        super(PropertyUtils.getProperty("common.test.pls.url"), "pls/datacollection/datafeed");
    }

    @Override
    protected String loginInternal(String username, String password) {
        throw new UnsupportedOperationException("We do not support login on this proxy yet.");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId consolidate() {
        String url = constructUrl("/consolidate");
        ResponseDocument<String> responseDoc = post("kickoff consolidate", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId profile() {
        String url = constructUrl("/profile");
        ResponseDocument<String> responseDoc = post("kickoff finalize", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

}
