angular.module('login', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.ResourceStringsService'
])
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    //$locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/form');

    $stateProvider
        .state('login', {
            url: '/',
            resolve: {
                ResourceStrings: function($q, BrowserStorageUtility, ResourceStringsService) {
                    var deferred = $q.defer(),
                        session = BrowserStorageUtility.getClientSession();

                    ResourceStringsService.GetExternalResourceStringsForLocale().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "header": {
                    template: ''
                },
                "main": {
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        },
                        ClientSession: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getClientSession() || {};
                        }
                    },
                    template: '<login-frame></login-frame>'
                }
            }
        })
        .state('login.form', {
            url: 'form',
            views: {
                "FrameContent": {
                    template: '<login-form></login-form>'
                }
            }
        })
        .state('login.tenants', {
            url: 'tenants',
            views: {
                "FrameContent": {
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        },
                        TenantList: function(LoginDocument) {
                            return LoginDocument.Tenants || [];
                        }
                    },
                    template: '<login-tenants></login-tenants>',
                }
            }
        })
        .state('login.forgot', {
            url: 'forgot',
            views: {
                "FrameContent": {
                    template: '<login-forgot-password></login-forgot-password>'
                }
            }
        })
        .state('login.update', {
            url: 'update',
            views: {
                "FrameContent": {
                    resolve: {
                        LoginDocument: function(BrowserStorageUtility) {
                            return BrowserStorageUtility.getLoginDocument() || {};
                        }
                    }, 
                    template: '<login-update-password></login-update-password>'
                }
            }
        })
        .state('login.success', {
            url: 'success',
            views: {
                "FrameContent": {
                    template: '<login-update-password-success></login-update-password-success>'
                }
            }
        });
});