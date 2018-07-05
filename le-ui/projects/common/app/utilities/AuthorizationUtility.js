angular.module('mainApp.core.utilities.AuthorizationUtility', [])  
.service('AuthorizationUtility', function(
    $q, $state, $stateParams,  $rootScope, BrowserStorageUtility, FeatureFlagService
){
    var AuthorizationUtility = this,
        flags = FeatureFlagService.Flags();


    this.init = function() {
        this.ClientSession = BrowserStorageUtility.getClientSession();
        this.clientAccessLevel = this.ClientSession.AccessLevel;

        //common access level groupings
        this.allAccessLevels = ['EXTERNAL_USER', 'EXTERNAL_ADMIN', 'INTERNAL_USER', 'INTERNAL_ADMIN', 'SUPER_ADMIN'];
        this.excludeExternalUser = ['EXTERNAL_ADMIN', 'INTERNAL_USER', 'INTERNAL_ADMIN', 'SUPER_ADMIN'];
        this.admins = ['EXTERNAL_ADMIN', 'INTERNAL_ADMIN', 'SUPER_ADMIN']; 
    }

    this.redirectIfNotAuthorized = function(accessLevels, featureFlags, redirectTo, params) {
        if (!this.checkAccessLevel(accessLevels) || !this.checkFeatureFlags(featureFlags)) {
            console.error('Unauthorized Access');
            $state.go(redirectTo, params);
        }
    }

    this.checkAccessLevel = function(accessLevels) {
        if (accessLevels != []) {
            return accessLevels.indexOf(this.clientAccessLevel) >= 0;
        } else {
            console.warn('Access levels are not defined');
        }
    }

    this.checkFeatureFlags = function(featureFlags) {
        if (featureFlags != {}) {
            for (var flag in featureFlags) {
                if (FeatureFlagService.FlagIsEnabled(flag) != featureFlags[flag]) {
                    return false;
                }
            }
            return true;
        } else {
            console.warn('Feature flags are not defined');
        }

    }

    this.clear = function() {
        this.init();
    }

    this.init();

})