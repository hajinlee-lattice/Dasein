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
            onExit: ['RatingsEngineStore', 'AtlasRemodelStore', function(RatingsEngineStore, AtlasRemodelStore) {
                RatingsEngineStore.init();
                AtlasRemodelStore.init();
            }],
            resolve: {
                WizardValidationStore: function (RatingsEngineStore) {
                    return RatingsEngineStore;
                },
                WizardProgressContext: function () {
                    return 'ratingsengine.remodel';
                },
                WizardProgressItems: function ($state, AtlasRemodelStore, RatingsEngineService, RatingsEngineStore, Banner) {
                    return [
                        { 
                            label: 'Training Changes', 
                            state: 'training', 
                            nextFn: function(nextState) {
                                $state.go(nextState);
                            }, 
                            progressDisabled: false 
                        },
                        { 
                            label: 'Attributes Enablement', 
                            state: 'training.attributes', 
                            nextFn: function(nextState) {
                                AtlasRemodelStore.saveIteration(nextState, 'attributes');
                            }, 
                            progressDisabled: false,
                            showNextSpinner: true
                        },
                        { 
                            label: 'Creation', 
                            state: 'training.attributes.creation', 
                            progressDisabled: true,
                            hideBack: true,
                            secondaryLinkLabel: 'Go to Model List',
                            secondaryLink: 'home.ratingsengine',
                            lastRoute: true,
                            nextLabel: 'Create another Model',
                            nextFn: function(nextState) {
                                $state.go('home.ratingsengine.ratingsenginetype');
                            }
                        }
                    ];
                },
                WizardContainerId: function () {
                    return 'ratingsengine';
                },
                WizardHeaderTitle: function ($stateParams, AtlasRemodelStore) {
                    var iteration = AtlasRemodelStore.getRemodelIteration(),
                        title = 'Remodel - Iteration ' + iteration.AI.iteration;
                        
                    return title;
                },
                WizardControlsOptions: function ($stateParams, AtlasRemodelStore) {

                    var iteration = AtlasRemodelStore.getRemodelIteration();

                    return {
                        backState: {
                            route: 'home.ratingsengine.dashboard', 
                            params: {
                                rating_id: $stateParams.engineId,
                                modelId: iteration.AI.modelSummaryId
                            }
                        },
                        nextState: 'home.ratingsengine.dashboard',
                        secondaryLink: 'home.ratingsengine'

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