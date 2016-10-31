package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchGuideBook;

public class DnbLookupActor extends DataSourceWrapperActorTemplate {
    private MatchGuideBook guideBook;
    private DnBLookupServiceImpl dnBLookupService;

    public DnbLookupActor(MatchGuideBook guideBook, DnBLookupServiceImpl dnBLookupService) {
        super();
        this.guideBook = guideBook;
        this.dnBLookupService = dnBLookupService;
    }

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected DataSourceLookupService getDataSourceLookupService() {
        return dnBLookupService;
    }

}
