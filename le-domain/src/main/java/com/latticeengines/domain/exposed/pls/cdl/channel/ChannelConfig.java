package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT)
@JsonSubTypes({ //
        @Type(value = SalesforceChannelConfig.class, name = "salesforce"), //
        @Type(value = MarketoChannelConfig.class, name = "marketo"), //
        @Type(value = EloquaChannelConfig.class, name = "eloqua"), //
        @Type(value = S3ChannelConfig.class, name = "aws_s3"), //
})
public interface ChannelConfig {

    ChannelConfig copyConfig(ChannelConfig config);

}
