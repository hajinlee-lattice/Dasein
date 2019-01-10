var app = angular.module('TenantConsoleApp', [
    'ui.router',
    'LocalStorageModule',
    'le.common.util.BrowserStorageUtility',
    'le.common.filter.filters',
    'app.core.directive.MainNavDirective',
    'app.login.controller.LoginCtrl',
    'app.tenants.controller.TenantListCtrl',
    'app.tenants.controller.TenantConfigCtrl',
    'app.modelquality',
    'app.datacloud',
    'lp.navigation.pagination',
    'templates-main'
]);

app.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
    return {
        request: function (config) {
            config.headers = config.headers || {};
            if (BrowserStorageUtility.getTokenDocument()) {
                config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
            }
            return config;
        },
        response: function (response) {
            if (response.status === 401) {
                // handle the case where the user is not authenticated
                $window.location.href='/';
            }
            return response || $q.when(response);
        }
    };
});

app.factory('jsonInterceptor', function () {
    return {
        request: function (config) {
            config.headers = config.headers || {};
            config.headers['Content-Type'] = 'application/json';
            return config;
        }
    };
});

app.config(function($stateProvider, $urlRouterProvider, $httpProvider, localStorageServiceProvider) {

    $httpProvider.interceptors.push('authInterceptor');
    $httpProvider.interceptors.push('jsonInterceptor');

    $urlRouterProvider.when('', '/login');
    $urlRouterProvider.when('/tenants', '/tenants/');
    $urlRouterProvider.when('/modelquality', '/modelquality/dashboard');
    $urlRouterProvider.when('/modelquality/', '/modelquality/dashboard');

    // For any unmatched url, redirect to
    $urlRouterProvider.otherwise('/');

    // define states of the app
    $stateProvider
        .state('LOGIN', {
            url: '/login',
            templateUrl: 'app/login/view/LoginView.html'
        })
        .state('TENANT', {
            url: '/tenants',
            templateUrl: 'app/core/view/MainBaseView.html'
        })
        .state('TENANT.LIST', {
            url: '/',
            templateUrl: 'app/tenants/view/TenantListView.html'
        })
        .state('TENANT.CONFIG', {
            url: '/{tenantId}?contractId&new&readonly&listenState',
            templateUrl: 'app/tenants/view/TenantConfigView.html'
        })
        .state('MODELQUALITY', {
            url: '/modelquality',
            views: {
                '': {
                    templateUrl: 'app/modelquality/view/ModelQualityRootView.html',
                    controller: 'ModelQualityRootCtrl'
                }
            }
        })
        .state('MODELQUALITY.DASHBOARD', {
            url: '/dashboard',
            views: {
                'main@MODELQUALITY': {
                    templateUrl: 'app/modelquality/dashboard/view/DashboardView.html',
                    controller: 'ModelQualityDashboardCtrl'
                }
            },
            resolve: {
                AnalyticTests: function (ModelQualityService) {
                    return ModelQualityService.GetAllAnalyticTests();
                }
            }
        })
        .state('MODELQUALITY.ANALYTICPIPELINE', {
            url: '/analyticpipeline',
            views: {
                'main@MODELQUALITY': {
                    templateUrl: 'app/modelquality/analyticpipeline/view/AnalyticPipelineView.html',
                    controller: 'AnalyticPipelineCtrl',
                    controllerAs: 'vm_analyticPipeline'
                },
                'createPipelineStep@MODELQUALITY.ANALYTICPIPELINE': {
                    templateUrl: 'app/modelquality/pipeline/view/PipelineStepView.html',
                    controller: 'PipelineStepCtrl',
                    controllerAs: 'vm_pipelineStep'
                }
            },
            resolve: {
                AnalyticPipelines: function (ModelQualityService) {
                    return ModelQualityService.GetAllAnalyticPipelines();
                },
                PropDataConfigs: function (ModelQualityService) {
                    return ModelQualityService.PropDataLatestForUI();
                }
            }
        })
        .state('MODELQUALITY.ANALYTICTEST', {
            url: '/analytictest',
            views: {
                'main@MODELQUALITY': {
                    templateUrl: 'app/modelquality/analytictest/view/AnalyticTestView.html',
                    controller: 'AnalyticTestCtrl',
                    controllerAs: 'vm_analyticTest'
                }
            },
            resolve: {
                AnalyticTests: function (ModelQualityService) {
                    return ModelQualityService.GetAllAnalyticTests();
                },
                AnalyticPipelines: function (ModelQualityService) {
                    return ModelQualityService.GetAllAnalyticPipelines();
                },
                Datasets: function (ModelQualityService) {
                    return ModelQualityService.GetAllDatasets();
                },
                AnalyticTestTypes: function (ModelQualityService) {
                    return ModelQualityService.GetAnalyticTestTypes();
                }
            }
        })
        .state('MODELQUALITY.PUBLISHLATEST', {
            url: '/publishlatest',
            views: {
                'main@MODELQUALITY': {
                    templateUrl: 'app/modelquality/publishlatest/view/PublishLatestView.html',
                    controller: 'PublishLatestCtrl',
                    controllerAs: 'vm_publishLatest'
                }
            }
        })
        .state('DATACLOUD', {
            url: '/datacloud',
            views: {
                '': {
                    templateUrl: 'app/datacloud/view/DataCloudRootView.html',
                    controller: 'DataCloudRootCtrl'
                }
            }
        })
        .state('DATACLOUD.METADATA', {
            url: '/metadata',
            views: {
                'main@DATACLOUD': {
                    templateUrl: 'app/datacloud/metadata/view/MetadataView.html',
                    controller: 'MetadataCtrl'
                }
            }
        })
        .state('DATACLOUD.METADATA2', {
            url: '/metadata2/{version}',
            views: {
                'main@DATACLOUD': {
                    templateUrl: 'app/datacloud/metadata2/view/Metadata2View.html',
                    controller: 'Metadata2Ctrl'
                }
            }
        })
        .state('NOWHERE', {
            url: '/',
            templateUrl: 'app/core/view/Http404View.html',
            params: {
                error: false,
                pageMessage: null
            },
            controller: function ($scope, $stateParams) {
                if ($stateParams.pageMessage) {
                    $scope.pageMessage = $stateParams.pageMessage;
                } else if ($stateParams.error) {
                    $scope.pageMessage = '500 (Internal Server Error)';
                } else {
                    $scope.pageMessage = '404 Page Not Found';
                }
            }
        });

    localStorageServiceProvider
        .setPrefix('lattice-engines')
        .setStorageType('sessionStorage')
        .setNotify(true, true);
});

app.run(function($rootScope, $state) {
    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error) {
        event.preventDefault();

        var pageMessage = null;
        if (error && error.errMsg) {
            pageMessage = error.errMsg.errorCode + ': ' + error.errMsg.errorMsg;
        }

        $state.go('NOWHERE', {error: true, pageMessage: pageMessage});
    });
});

app.config(function($provide) {
    $provide.decorator('$httpBackend', function($delegate) {
        return function(method, url, post, callback, headers, timeout, withCredentials, responseType) {
            // multi select to influxdb require semicolon to be encoded
            url = url.replace(/;/g, '%3B');
            $delegate(method, url, post, callback, headers, timeout, withCredentials, responseType);
        };
    });
});
