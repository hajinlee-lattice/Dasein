import {
    UIRouterReact,
    servicesPlugin,
    hashLocationPlugin
} from "common/react-vendor";
import mainStates from "./mainstates";
import { actions as bannerActions } from 'common/widgets/banner/le-banner.redux';
import { store } from 'store';

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
        const states = mainStates;
        //console.log('STATE ',states);
        states.forEach(state => this.routing.router.stateRegistry.register(state));

        // Set initial and fallback states
        // this.router.urlService.rules.initial();

        this.routing.router.transitionService.onBefore(true, function (trans) {
            // console.log("Nav Start");
            // Do something before transition
        });

        this.routing.router.transitionService.onSuccess(true, function (trans) {
            // // Do something after transition
            bannerActions.clearBanners(store);
            // bannerActions.closeBanner(store);
        });

        this.routing.router.transitionService.onError(true, function (err) {
            // console.log("Nav Error", err);
            // Do something if transition errors
        });
    }

    getRouter() {
        if (this.routing.router) {
            return this.routing.router;
        } else {
            this.initRouter();
            return this.routing.router;
        }
    }
    getStateService() {
        return this.routing.router.stateService;
    }
    getCurrentState() {
        // console.log('SS ==> ',this.routing.router.stateService);
        let currentState = this.routing.router.stateService.current;
        // console.log('CURRENT ',currentState);
        return currentState;
    }
    /**
     * 
     */
    clear() {
        delete this.routing.router;
    }

}

const instance = new ReactRouter();
Object.freeze(instance);

export default instance;

