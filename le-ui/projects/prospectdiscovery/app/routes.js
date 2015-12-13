angular
    .module('mainApp')

    // add ability to redirect with redirectTo
    .run(['$rootScope', '$state', function($rootScope, $state) {
        $rootScope.$on('$stateChangeStart', function(evt, to, params) {
          if (to.redirectTo) {
            evt.preventDefault();
            $state.go(to.redirectTo, params)
          }
        });
    }])

    // define routes for PD application.
    .config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
        $urlRouterProvider.otherwise('/');

        $stateProvider
            .state('home', {
                url: '/',
                redirectTo: 'markets'
            })
            .state('fingerprints', {
                url: '/fingerprints',
                views: {
                     "main": {
                        templateUrl: './app/fingerprints/FingerprintsView.html'
                    }
                }
            })
            .state('markets', {
                url: '/markets',
                redirectTo: 'markets.dashboard'
            })
            .state('markets.dashboard', {
                url: '/dashboard',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/navdash/NavDashView.html'
                    },
                    "main@": {
                        templateUrl: './app/markets/dashboard/DashboardView.html'
                    }
                }
            })
            .state('markets.list', {
                url: '/status',
                views: {
                    "summary@": {
                        template: ''
                    },
                    "main@": {
                        templateUrl: './app/markets/MarketsView.html'
                    }
                }
            })
            .state('builder', {
                url: '/builder',
                redirectTo: 'builder.industries',
                views: {
                    "navigation@": {
                        templateUrl: './app/navigation/sidebar/BuilderView.html'
                    },
                    "summary@": {
                        templateUrl: './app/navigation/subnav/SubNavView.html'
                    }
                }
            })
            .state('builder.industries', {
                url: '/industries',
                views: {
                    "main@": {
                        templateUrl: './app/builder/industries/IndustriesView.html'
                    }
                }
            })
            .state('builder.locations', {
                url: '/locations',
                views: {
                    "main@": {
                        templateUrl: './app/builder/locations/LocationsView.html'
                    }
                }
            })
            .state('builder.firmographics', {
                url: '/firmographics',
                views: {
                    "main@": {
                        templateUrl: './app/builder/firmographics/FirmographicsView.html'
                    }
                }
            })
            .state('builder.growthtrends', {
                url: '/growthtrends',
                views: {
                    "main@": {
                        templateUrl: './app/builder/growthtrends/GrowthTrendsView.html'
                    }
                }
            })
            .state('builder.technology', {
                url: '/technology',
                views: {
                    "main@": {
                        templateUrl: './app/builder/technology/TechnologyView.html'
                    }
                }
            })
            .state('builder.webpresence', {
                url: '/webpresence',
                views: {
                    "main@": {
                        templateUrl: './app/builder/webpresence/WebPresenceView.html'
                    }
                }
            })
            .state('builder.other', {
                url: '/other',
                views: {
                    "main@": {
                        templateUrl: './app/builder/other/OtherView.html'
                    }
                }
            })
            .state('markets.prospect_schedule', {
                url: '/prospect_schedule',
                views: {
                    "summary@": {
                        template: ''
                    },
                    "main@": {
                        templateUrl: './app/markets/prospect/ScheduleView.html'
                    }
                }
            })
            .state('markets.prospect_list', {
                url: '/prospect_list',
                views: {
                    "summary@": {
                        template: ''
                    },
                    "main@": {
                        templateUrl: './app/markets/prospect/ListView.html'
                    }
                }
            })
            .state('jobs', {
                url: '/jobs',
                redirectTo: 'jobs.status'
            })
            .state('jobs.status', {
                url: '/status',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/table/TableView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/status/StatusView.html'
                    }
                }
            })
            .state('jobs.import', {
                url: '/import'
            })
            .state('jobs.import.credentials', {
                url: '/credentials',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/credentials/CredentialsView.html'
                    }
                }
            })
            .state('jobs.import.file', {
                url: '/file',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/file/FileView.html'
                    }
                }
            })
            .state('jobs.import.processing', {
                url: '/processing',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/message/MessageView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/processing/ProcessingView.html'
                    }
                }
            })
            .state('jobs.import.ready', {
                url: '/ready/:jobId',
                views: {
                    "summary@": {
                        templateUrl: './app/navigation/table/TableView.html'
                    },
                    "main@": {
                        templateUrl: './app/jobs/import/ready/ReadyView.html'
                    }
                }
            });
        }
    ]
);