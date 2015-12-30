angular
    .module('pd.markets')
    .config(['$stateProvider', function($stateProvider) {
        $stateProvider
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
            .state('markets.create', {
                url: '/create',
                views: {
                    "summary@": {
                        template: ''
                    },
                    "main@": {
                        templateUrl: './app/markets/createmarket/CreateMarketView.html'
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
            });
        }
    ]);