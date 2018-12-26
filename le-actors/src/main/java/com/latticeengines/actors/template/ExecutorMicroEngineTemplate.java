package com.latticeengines.actors.template;

/**
 * Actors in decision graph have 3 types: anchor, micro-engine & junction
 * 
 * Anchor is entry/exit actor
 * 
 * Micro-engine is actors where traveler travels around. Micro-engine has 2
 * types: proxy micro-engine & executor micro-engine. Proxy micro-engine needs
 * assistant acotr to finish task. Task micro-engine does the task by itself.
 * 
 * Junction is the connecting point between decision graph/actor system
 */
public abstract class ExecutorMicroEngineTemplate extends VisitorActorTemplate {
    // TODO(ZDD): to implement
}
