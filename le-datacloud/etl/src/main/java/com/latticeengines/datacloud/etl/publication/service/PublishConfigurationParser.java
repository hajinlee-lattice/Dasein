package com.latticeengines.datacloud.etl.publication.service;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;

public interface PublishConfigurationParser {

    PublishToDynamoConfiguration parseDynamoAlias(PublishToDynamoConfiguration dynamoConfiguration);

    DynamoService constructDynamoService(PublishToDynamoConfiguration dynamoConfiguration);
}
