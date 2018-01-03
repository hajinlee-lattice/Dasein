angular.module('login', [
    'mainApp.appCommon.directives.ngEnterDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.ResourceStringsService'
])
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
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
        })
        .state('login.saml', {
            url: 'saml/:tenantId',
            params: {
                noLogoArea: true
            },
            views: {
                "FrameContent": {
                    resolve: {
                        Saml: function($q) {
                            var deferred = $q.defer();

                            deferred.resolve([]);

                            return deferred.promise;
                        },
                    },
                    template: '<login-saml></login-saml>'
                }
            }
        })
        .state('login.saml.logout', { // FIXME route not working, might be caused by some redirect handling somewhere
             url: 'logout',
            views: {
                "FrameContent": {
                    resolve: {
                        Saml: function($q) {
                            var deferred = $q.defer();

                            deferred.resolve([]);

                            return deferred.promise;
                        },
                    },
                    template: '<login-saml-logout></login-saml-logout>'
                }
            }
        })
        .state('login.saml.error', {
            url: ':tenantId/error',
            views: {
                "FrameContent": {
                    resolve: {
                        Saml: function($q) {
                            var deferred = $q.defer();

                            deferred.resolve([]);

                            return deferred.promise;
                        },
                    },
                    template: '<login-saml-metadata></login-saml-metadata>'
                }
            }
        });
});