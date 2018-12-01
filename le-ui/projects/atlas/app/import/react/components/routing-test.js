import React, {
    UIRouterReact,
    servicesPlugin,
    hashLocationPlugin
} from "common/react-vendor";
import { hello, about, templatelist } from "../states";

var router;

export const getRouter = () => {
    if (!router) {
        router = new UIRouterReact();
        router.plugin(servicesPlugin);
        router.plugin(hashLocationPlugin);

        // Register each state
        const states = [hello, about, templatelist];
        states.forEach(state => router.stateRegistry.register(state));

        // Set initial and fallback states
        router.urlService.rules.initial({ state: "templatelist" });

        router.transitionService.onBefore(true, function(trans) {
            // console.log("Nav Start");
            // Do something before transition
        });

        router.transitionService.onSuccess(true, function(trans) {
            // console.log("Nav End");
            // Do something after transition
        });

        router.transitionService.onError(true, function(err) {
            // console.log("Nav Error", err);
            // Do something if transition errors
        });
    }
    // else {
    // console.log("ALREADY INIT");
    // }

    return router;
};
export const clean = () => {
    router = null;
};
