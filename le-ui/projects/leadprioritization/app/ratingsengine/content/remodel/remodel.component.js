angular.module('lp.ratingsengine.remodel', [
    'lp.ratingsengine.remodel.training',
    'lp.ratingsengine.remodel.attributes',
    'lp.ratingsengine.remodel.creation',
    'lp.ratingsengine.remodel.list',
    'lp.ratingsengine.remodel.filters'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine.remodel', {
            url: '/:engineId/remodel/:modelId',
            onExit: ['RatingsEngineStore', function(RatingsEngineStore) {
                RatingsEngineStore.init();
            }],
            resolve: {
                WizardValidationStore: function (RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function () {
                    return 'ratingsengine.remodel';
                },
                WizardProgressItems: function ($stateParams, AtlasRemodelStore) {
                    return [
                        { label: 'Training Changes', state: 'training', nextFn: '', progressDisabled: false },
                        { label: 'Available Attributes', state: 'training.attributes', nextFn: AtlasRemodelStore.saveIteration, progressDisabled: false },
                        { label: 'Creation', state: 'training.attributes.creation', nextFn: '', progressDisabled: true }
                    ];
                },
                WizardContainerId: function () {
                    return 'ratingsengine';
                },
                WizardHeaderTitle: function () {
                    return 'Remodel';
                },
                WizardCustomHeaderSteps: function() {
                    return null;
                },
                DisableWizardNavOnLastStep: function () {
                    return null;
                },
                WizardControlsOptions: function () {
                    return {
                        backState: 'home.ratingsengine',
                        nextState: 'home.ratingsengine.dashboard'
                    };
                }
            },
            views: {
                'summary@': {
                    templateUrl: 'app/navigation/summary/BlankLine.html'
                },
                'main@': {
                    controller: 'ImportWizard',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/wizard.component.html'
                },
                'wizard_header@home.ratingsengine.remodel': {
                    controller:'WizardHeader',
                    controllerAs:'vm',
                    templateUrl: '/components/wizard/header/header.component.html'
                },
                'wizard_progress@home.ratingsengine.remodel': {
                    controller: 'ImportWizardProgress',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/progress/progress.component.html'
                },
                'wizard_controls@home.ratingsengine.remodel': {
                    controller: 'ImportWizardControls',
                    controllerAs: 'vm',
                    templateUrl: '/components/wizard/controls/controls.component.html'
                }
            },
            redirectTo: 'home.ratingsengine.remodel.training'
        });
})
.component('remodel', {
    template: '',
    controller: function () {}
});