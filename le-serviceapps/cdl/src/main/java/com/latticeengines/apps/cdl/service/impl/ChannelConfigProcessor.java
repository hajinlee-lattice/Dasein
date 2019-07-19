package com.latticeengines.apps.cdl.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;

@Component
public class ChannelConfigProcessor {

    public void postProcessChannelConfig(ChannelConfig channelConfig) {
        return;
    }

    public void postProcessChannelConfig(S3ChannelConfig s3ChannelConfig) {
        return;
    }

}
