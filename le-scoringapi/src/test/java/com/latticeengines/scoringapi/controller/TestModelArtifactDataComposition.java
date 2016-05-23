package com.latticeengines.scoringapi.controller;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;

public class TestModelArtifactDataComposition {
    private DataComposition eventTableDataComposition;

    private DataComposition dataScienceDataComposition;

    public DataComposition getEventTableDataComposition() {
        return eventTableDataComposition;
    }

    public void setEventTableDataComposition(DataComposition eventTableDataComposition) {
        this.eventTableDataComposition = eventTableDataComposition;
    }

    public DataComposition getDataScienceDataComposition() {
        return dataScienceDataComposition;
    }

    public void setDataScienceDataComposition(DataComposition dataScienceDataComposition) {
        this.dataScienceDataComposition = dataScienceDataComposition;
    }

}
