import {
    UIRouterReact,
    servicesPlugin,
    hashLocationPlugin,
    pushStateLocationPlugin
} from "common/react-vendor";
import {profiles } from "./states";

// window['reactrouter'] =var router;

export const getRouter = () => {
    let router = window['reactrouter'];
    if (!router || router == null) {
        router = new UIRouterReact();
        console.log(router);
        // router.html5Mode(true);
        router.plugin(servicesPlugin);
        router.plugin(hashLocationPlugin);
        // router.plugin(pushStateLocationPlugin);
        // Register each state
        const states = [profiles];
        states.forEach(state => router.stateRegistry.register(state));

        // Set initial and fallback states
        // router.urlService.rules.initial({ state: "profiles" });

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
        window['reactrouter'] = router
    }

    return router;
};
export const clean = () => {
    window['reactrouter'] = null;
};
