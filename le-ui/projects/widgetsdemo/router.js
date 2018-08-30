import { UIRouterReact, servicesPlugin, hashLocationPlugin } from '@uirouter/react';

// Module states
import buttons from './modules/buttons/state';
import dropdowns from './modules/dropdowns/state';
import menus from './modules/menus/state';

// Create instance + router setup
const router = new UIRouterReact();
router.plugin(servicesPlugin);
router.plugin(hashLocationPlugin);

// Register each state
const states = [
    buttons,
    dropdowns,
    menus
];
states.forEach(state => router.stateRegistry.register(state));

// Set initial and fallback states
router.urlService.rules.initial({ state: 'buttons' });
router.urlService.rules.initial({ state: 'dropdowns' });
router.urlService.rules.initial({ state: 'menus' });

router.transitionService.onBefore(true, function(trans) {
    console.log('Nav Start');
    // Do something before transition
});

router.transitionService.onSuccess(true, function(trans) {
    console.log('Nav End');
    // Do something after transition
});

router.transitionService.onError(true, function(err) {
    console.log('Nav Error', err);
    // Do something if transition errors
});


export default router;