package com.latticeengines.datacloud.match.actors;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.latticeengines.actors.ActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

public class SingleActorTestNGBase extends DataCloudMatchFunctionalTestNGBase {

    // IMPORTANT
    // Should not run tests in parallel which need different mode
    // (batch/realtime) for actorSystem
    @Inject
    protected MatchActorSystem actorSystem;

    protected Object sendMessageToActor(Object msg, Class<? extends ActorTemplate> actorClazz, boolean batchMode)
            throws Exception {
        LogManager.getLogger("com.latticeengines.datacloud.match.actors.visitor").setLevel(Level.DEBUG);
        LogManager.getLogger("com.latticeengines.actors.visitor").setLevel(Level.DEBUG);

        ActorRef actorRef = actorSystem.getActorRef(actorClazz);

        Timeout timeout = batchMode ? new Timeout(new FiniteDuration(30, TimeUnit.MINUTES))
                : new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
        Future<Object> future = Patterns.ask(actorRef, msg, timeout);

        Object result = Await.result(future, timeout.duration());
        return result;
    }
}
