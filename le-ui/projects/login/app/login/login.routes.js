angular.module('login', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.ResourceStringsService'
])
.config(function($stateProvider) {
    $stateProvider
        .state('login', {
            url: '/',
            onEnter: function(ResourceStringsService) {
                ResourceStringsService.GetExternalResourceStringsForLocale();
            },
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                },
                clientsession: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getClientSession() || {};
                }
            },
            views: {
                "main": "loginFrame"
            }
        })
        .state('login.form', {
            url: 'form',
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                }
            },
            views: {
                "FrameContent": "loginForm"
            }
        })
        .state('login.tenants', {
            url: 'tenants',
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                },
                tenantlist: function(logindocument) {
                    return logindocument.Tenants || [];
                }
            },
            views: {
                "FrameContent": "loginTenants"
            }
        })
        .state('login.forgot', {
            url: 'forgot',
            views: {
                "FrameContent": "loginForgotPassword"
            }
        })
        .state('login.update', {
            url: 'update',
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                }
            }, 
            views: {
                "FrameContent": "loginUpdatePassword"
            }
        })
        .state('login.success', {
            url: 'success',
            views: {
                "FrameContent": "loginUpdatePasswordSuccess"
            }
        })
        .state('login.saml', {
            url: 'saml/:tenantId',
            params: {
                disableLogoArea: true
            },
            views: {
                "FrameContent": "loginSaml"
            }
        })
        .state('login.saml_logout', { 
            url: 'saml/:tenantId/logout',
            params: {
                disableLogoArea: true
            },
            views: {
                "FrameContent": "loginSamlLogout"
            }
        })
        .state('login.saml_error', {
            url: 'saml/:tenantId/error',
            params: {
                disableLogoArea: true
            },
            views: {
                "FrameContent": "loginSamlError"
            }
        });
});