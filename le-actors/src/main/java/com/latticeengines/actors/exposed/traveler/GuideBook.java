package com.latticeengines.actors.exposed.traveler;

public interface GuideBook {
    String next(String currentLocation, TravelerContext traveler);

    String getDataSourceActorPath(String dataSourceName);
}
