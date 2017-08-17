angular
.module('lp.ratingsengine', [
    'common.wizard',
    'lp.ratingsengine.ratingslist',
    'lp.ratingsengine.dashboard'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine', {
            url: '/ratings_engine',
            redirectTo: 'home.ratingsengine.ratingslist'
        })
        .state('home.ratingsengine.ratingslist', {
            url: '/ratings',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Ratings Engine'
            },
            views: {
                "main@": {
                    controller: 'RatingsEngineRatingsList',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/ratingslist/ratingslist.component.html'
                }
            }
        })
        .state('home.ratingsengine.dashboard', {
            url: '/dashboard/:rating_id',
            params: {
                pageIcon: 'ico-playbook',
                pageTitle: 'Ratings Engine'
            },
            views: {
                "navigation@": {
                    controller: function($scope, $stateParams, $state, $rootScope) {
                        $rootScope.$broadcast('header-back', { 
                            path: '^home.rating.dashboard',
                            displayName: 'Rating name',
                            sref: 'home.ratingsengine'
                        });
                    },
                    templateUrl: 'app/ratingsengine/content/dashboard/sidebar/sidebar.component.html'
                },
                'main@': {
                    controller: 'RatingsEngineDashboard',
                    controllerAs: 'vm',
                    templateUrl: 'app/ratingsengine/content/dashboard/dashboard.component.html'
                }
            }
        });
});