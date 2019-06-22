package com.latticeengines.aws.firehose;

import java.util.List;

public interface FirehoseService {

    void send(String deliveryStreamName, String s3ObjectPrefix, String stream);

    void sendBatch(String deliveryStreamName, String s3ObjectPrefix, List<String> streams);

}
