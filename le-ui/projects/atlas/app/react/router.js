import {
    UIRouterReact,
    servicesPlugin,
    hashLocationPlugin
} from "common/react-vendor";
import appState  from "./states";

class ReactRouter {
    constructor() {
        if (!ReactRouter.instance || !ReactRouter.instance.routing.router) {
            ReactRouter.instance = this;
            this.routing = {};
            this.initRouter();
        }

        return ReactRouter.instance;
    }
    initRouter() {
        this.routing.router = new UIRouterReact();
        this.routing.router.plugin(servicesPlugin);
        this.routing.router.plugin(hashLocationPlugin);

        // Register each state
        const states = appState;
        states.forEach(state => this.routing.router.stateRegistry.register(state));

        // Set initial and fallback states
        // this.router.urlService.rules.initial();

        this.routing.router.transitionService.onBefore(true, function (trans) {
            // console.log("Nav Start");
            // Do something before transition
        });

        this.routing.router.transitionService.onSuccess(true, function (trans) {
            // console.log("Nav End");
            // Do something after transition
        });

        this.routing.router.transitionService.onError(true, function (err) {
            // console.log("Nav Error", err);
            // Do something if transition errors
        });
    }

    getRouter() {
        if(this.routing.router){
            return this.routing.router;
        }else{
            this.initRouter();
            return this.routing.router;
        }
    }
    getStateService() {
        return this.routing.router.stateService;
    }
    getCurrentState() {
        console.log('SS ==> ',this.routing.router.stateService);
        let currentState = this.routing.router.stateService.current;
        console.log('CURRENT ',currentState);
        return currentState;
    }
    clear() {
        delete this.routing.router;
    }

}

const instance = new ReactRouter();
Object.freeze(instance);

export default instance;

