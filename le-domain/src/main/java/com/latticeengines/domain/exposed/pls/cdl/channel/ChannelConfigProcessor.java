package com.latticeengines.domain.exposed.pls.cdl.channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Component
public class ChannelConfigProcessor {

    private DropBoxSummary dropBoxSummary;
    private HdfsToS3PathBuilder pathBuilder;

    @Value("${aws.customer.export.s3.bucket}")
    private String exportS3Bucket;

    public void postProcessChannelConfig(ChannelConfig channelConfig) {
        return;
    }

    public void postProcessChannelConfig(S3ChannelConfig s3ChannelConfig) {
        s3ChannelConfig.setS3CampaignExportDir(
                pathBuilder.getS3CampaignExportDir(exportS3Bucket, dropBoxSummary.getDropBox()));
        return;
    }

}
