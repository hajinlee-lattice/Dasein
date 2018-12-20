package com.latticeengines.domain.exposed.datacloud.match.utils;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.actors.ActorType;

public class MatchActorUtils {
    /**
     * Add suffix to actor name if needed: AnchorActor, MicroEngineActor,
     * JunctionActor
     * 
     * @param actorName
     * @param mode
     * @return
     */
    public static String getFullActorName(@NotNull String actorName, @NotNull ActorType type) {
        Preconditions.checkNotNull(actorName);
        Preconditions.checkNotNull(type);

        return actorName.endsWith(type.getName()) ? actorName : actorName + type.getName();
    }

    /**
     * Remove suffix from actor name if needed: AnchorActor, MicroEngineActor,
     * JunctionActor
     * 
     * @param actorName
     * @param mode
     * @return
     */
    public static String getShortActorName(@NotNull String actorName, @NotNull ActorType type) {
        Preconditions.checkNotNull(actorName);
        Preconditions.checkNotNull(type);

        return actorName.endsWith(type.getName()) ? actorName.substring(0, actorName.indexOf(type.getName()))
                : actorName;
    }
}
