package com.latticeengines.aws.firehose;

import java.util.List;

public interface FirehoseService {

    void send(String deliveryStreamName, String stream);

    void sendBatch(String deliveryStreamName, List<String> streams);

}
