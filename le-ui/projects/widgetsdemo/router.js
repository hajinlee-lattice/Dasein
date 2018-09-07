import { UIRouterReact, servicesPlugin, hashLocationPlugin } from '../common/react-vendor';

// Module states
import buttons from './modules/buttons/state';
import dropdowns from './modules/dropdowns/state';
import menus from './modules/menus/state';
import panels from './modules/panels/state';
import grid from './modules/grid/state';
import inputs from './modules/inputs/state';


// Create instance + router setup
const router = new UIRouterReact();
router.plugin(servicesPlugin);
router.plugin(hashLocationPlugin);

// Register each state
const states = [
    buttons,
    dropdowns,
    menus,
    panels,
    grid,
    inputs
];
states.forEach(state => router.stateRegistry.register(state));

// Set initial and fallback states
router.urlService.rules.initial({ state: 'buttons' });
router.urlService.rules.initial({ state: 'dropdowns' });
router.urlService.rules.initial({ state: 'menus' });
router.urlService.rules.initial({ state: 'panels' });
router.urlService.rules.initial({ state: 'grid' });
router.urlService.rules.initial({ state: 'inputs' });

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