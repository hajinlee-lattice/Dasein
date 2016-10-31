package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchGuideBook;

public class DynamoLookupActor extends DataSourceWrapperActorTemplate {
    private MatchGuideBook guideBook;
    private DataSourceLookupService dynamoDBLookupService;

    public DynamoLookupActor(MatchGuideBook guideBook, DataSourceLookupService dynamoDBLookupService) {
        super();
        this.guideBook = guideBook;
        this.dynamoDBLookupService = dynamoDBLookupService;
    }

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dynamoDBLookupService;
    }

    @Override
    protected boolean shouldDoAsyncLookup() {
        return true;
    }
}
