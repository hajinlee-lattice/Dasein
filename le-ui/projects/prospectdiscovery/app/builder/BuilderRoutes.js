angular
    .module('pd.builder')
    .config(['$stateProvider', function($stateProvider) {
        $stateProvider
            .state('builder', {
                url: '/builder',
                redirectTo: "builder.category({AttrKey: 'Industry', ParentValue: ''})",
                views: {
                    "navigation@": {
                        templateUrl: './app/navigation/sidebar/BuilderView.html'
                    },
                    "summary@": {
                        templateUrl: './app/navigation/subnav/SubNavView.html'
                    }
                }
            })
            .state('builder.category', {
                url: '/categories/:AttrKey/:ParentKey/:ParentValue',
                views: {
                    "main@": {
                        templateUrl: './app/builder/category/CategoryView.html'
                    }
                }
            })
            .state('builder.intent', {
                url: '/intent/:AttrKey/:ParentKey/:ParentValue',
                views: {
                    "main@": {
                        templateUrl: './app/builder/category/CategoryView.html'
                    }
                }
            })
            .state('builder.filtercontacts', {
                url: '/filter_contacts',
                views: {
                    "main@": {
                        templateUrl: './app/builder/prospects/FilterContactsView.html'
                    },
                    "summary@": {
                        template: ''
                    }
                }
            })
            .state('builder.fitmodel', {
                url: '/fit_model',
                views: {
                    "main@": {
                        templateUrl: './app/builder/prospects/BuildFitModelView.html'
                    },
                    "summary@": {
                        template: ''
                    }
                }
            })
            .state('builder.intentmodel', {
                url: '/intent_model',
                views: {
                    "main@": {
                        templateUrl: './app/builder/prospects/BuildIntentModelView.html'
                    },
                    "summary@": {
                        template: ''
                    }
                }
            })
            .state('builder.setup', {
                url: '/setup',
                views: {
                    "main@": {
                        templateUrl: './app/builder/prospects/SetupProspectingView.html'
                    },
                    "summary@": {
                        template: ''
                    }
                }
            });
        }
    ]);