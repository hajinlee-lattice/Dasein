angular.module('login', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.ResourceStringsService'
])
.run(function($transitions) {
    $transitions.onStart({}, function(trans) {
        trans.injector().get('Banner').reset();
    });
})
.config(function($stateProvider) {
    $stateProvider
        .state('login', {
            url: '/',
            params: {
                disableUserInfo: true,
                disableLogoArea: false
            },
            resolve: {
                strings: function(ResourceStringsService) {
                    return ResourceStringsService.GetExternalResourceStringsForLocale();
                },
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                },
                clientsession: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getClientSession() || {};
                },
                params: function($stateParams){
                    return $stateParams;
                }
            },
            views: {
                "notice": "noticeMessage",
                "main": "loginFrame"
            }
        })
        .state('login.form', {
            url: 'form',
            params: {
                obj: {}
            },
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                }
            },
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginForm"
            }
        })
        .state('login.tenants', {
            url: 'tenants',
            params: {
                obj: {},
                disableUserInfo: false
            },
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                },
                tenantlist: function(logindocument) {
                    return logindocument.Tenants || [];
                }
            },
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginTenants"
            }
        })
        .state('login.forgot', {
            url: 'forgot',
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginForgotPassword"
            }
        })
        .state('login.update', {
            url: 'update',
            params: {
                disableUserInfo: true
            },
            resolve: {
                logindocument: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getLoginDocument() || {};
                }
            }, 
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginUpdatePassword"
            }
        })
        .state('login.success', {
            url: 'success',
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginUpdatePasswordSuccess"
            }
        })
        .state('login.saml', {
            url: 'saml/:tenantId',
            params: {
                disableUserInfo: false,
                disableLogoArea: true
            },
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginSaml"
            }
        })
        .state('login.saml_logout', { 
            url: 'saml/:tenantId/logout',
            params: {
                disableUserInfo: true,
                disableLogoArea: false
            },
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginSamlLogout"
            }
        })
        .state('login.saml_error', {
            url: 'saml/:tenantId/error',
            params: {
                disableUserInfo: true,
                disableLogoArea: false
            },
            views: {
                "banner": "bannerMessage",
                "FrameContent": "loginSamlError"
            }
        });
});