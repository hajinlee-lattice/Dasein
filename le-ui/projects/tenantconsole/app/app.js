var app = angular.module('TenantConsoleApp', [
    'ui.router',
    'LocalStorageModule',
    'le.common.util.BrowserStorageUtility',
    'app.core.directive.MainNavDirective',
    'app.login.controller.LoginCtrl',
    'app.tenants.controller.TenantListCtrl',
    'app.tenants.controller.TenantConfigCtrl',
    'app.modelquality',
    'app.datacloud'
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
                MeasurementData: function (InfluxDbService) {
                    var cols = [
                        'time',
                        'AlgorithmName',
                        'AnalyticPipelineName',
                        'AnalyticTestName',
                        'AnalyticTestTag',
                        'DataSetName',
                        'ModelID',
                        'PipelineName',
                        'PropDataConfigName',
                        'RocScore',
                        'SamplingName',
                        'Top10PercentLift',
                        'Top20PercentLift',
                        'Top30PercentLift'
                    ].join(',');
                    return InfluxDbService.Query({
                        q: 'SELECT ' + cols + ' FROM ModelingMeasurement',
                        db: 'ModelQuality'
                    });
                }
            }
        })
        .state('MODELQUALITY.CREATEPIPELINE', {
            url: '/createpipeline',
            views: {
                'main@MODELQUALITY': {
                    templateUrl: 'app/modelquality/pipeline/view/CreatePipelineView.html',
                    controller: 'ModelQualityCreatePipelineCtrl',
                    controllerAs: 'vm_createPipeline'
                },
                'createPipelineStep@MODELQUALITY.CREATEPIPELINE': {
                    templateUrl: 'app/modelquality/pipeline/view/CreatePipelineStepView.html',
                    controller: 'ModelQualityCreatePipelineStepCtrl',
                    controllerAs: 'vm_createPipelineStep'
                }
            },
            resolve: {
                Pipelines: function (ModelQualityService) {
                    return ModelQualityService.GetAllPipelines();
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
        .state('NOWHERE', {
            url: '/',
            templateUrl: 'app/core/view/Http404View.html'
        });

    localStorageServiceProvider
        .setPrefix('lattice-engines')
        .setStorageType('sessionStorage')
        .setNotify(true, true);
});