angular.module('common.utilities.SessionTimeout', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.login.services.LoginService'
])
.service('SessionTimeoutUtility', function (
    $rootScope, $state, $modal, $timeout, BrowserStorageUtility, LoginService
) {
    var vm = this;

    var TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS = 30 * 1000;
    var TIME_INTERVAL_INACTIVITY_BEFORE_WARNING = 14.5 * 60 * 1000;  // 14.5 minutes
    var TIME_INTERVAL_WARNING_BEFORE_LOGOUT = 30 * 1000;

    this.inactivityCheckingId = null;
    this.warningModalInstance = null;
    this.sessionExpired = false;

    vm.init = function() {
        vm.startObservingUserActivtyThroughMouseAndKeyboard();
        vm.startCheckingIfSessionIsInactive();
    }

    vm.refreshPreviousSession = function (tenant) {
        LoginService.GetSessionDocument(tenant).then(
            function (data, status) {
                if (data && data.Success === true) {
                    vm.startObservingUserActivtyThroughMouseAndKeyboard();
                    vm.startCheckingIfSessionIsInactive();
                }
            }
        );
    };

    vm.startObservingUserActivtyThroughMouseAndKeyboard = function() {
        $(document.body).mousemove(function (e) {
            if (!vm.warningModalInstance) {
                vm.refreshSessionLastActiveTimeStamp();
            }
        });

        $(document.body).keypress(function (e) {
            if (!vm.warningModalInstance) {
                vm.refreshSessionLastActiveTimeStamp();
            }
        });
    }

    vm.startCheckingIfSessionIsInactive = function() {
        vm.refreshSessionLastActiveTimeStamp();

        vm.inactivityCheckingId = setInterval(
            vm.checkIfSessionIsInactiveEveryInterval, 
            TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS
        );
    }

    vm.checkIfSessionIsInactiveEveryInterval = function() {
        var ignoreStates = [
            'home.models.import','home.models.pmml','home.model.scoring',
            'home.models.import.job','home.models.pmml.job'
        ];

        if (ignoreStates.indexOf($state.current.name) >= 0) {
            return;
        }

        if (Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >= TIME_INTERVAL_INACTIVITY_BEFORE_WARNING) {
            if (!vm.warningModalInstance) {
                vm.cancelCheckingIfSessionIsInactiveAndSetIdToNull();
                vm.openWarningModal();
            }

            $timeout(
                vm.callWhenWarningModalExpires, 
                TIME_INTERVAL_WARNING_BEFORE_LOGOUT
            );
        }
    }
    
    vm.refreshSessionLastActiveTimeStamp = function() {
        BrowserStorageUtility.setSessionLastActiveTimestamp(Date.now());
    }
    
    vm.hasSessionTimedOut = function() {
        return Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >=
            TIME_INTERVAL_INACTIVITY_BEFORE_WARNING + TIME_INTERVAL_WARNING_BEFORE_LOGOUT;
    }

    vm.openWarningModal = function() {
        vm.warningModalInstance = $modal.open({
            animation: true,
            backdrop: true,
            scope: $rootScope,
            templateUrl: '/app/modules/SessionTimeout/WarningModal.html'
        });

        $rootScope.refreshSession = function() {
            vm.closeWarningModalAndSetInstanceToNull();
            vm.startCheckingIfSessionIsInactive();
        };
    }
    
    vm.cancelCheckingIfSessionIsInactiveAndSetIdToNull = function() {
        clearInterval(vm.inactivityCheckingId);
        vm.inactivityCheckingId = null;
    }

    vm.stopObservingUserInteractionBasedOnMouseAndKeyboard = function() {
        $(document.body).off("mousemove");
        $(document.body).off("keypress");
    }
    
    vm.callWhenWarningModalExpires = function() {
        if (vm.hasSessionTimedOut()) {
            vm.sessionExpired = true;
            vm.stopObservingUserInteractionBasedOnMouseAndKeyboard();

            LoginService.Logout();
        } else {
            if (vm.warningModalInstance) {
                closeWarningModalAndSetInstanceToNull();
            }

            if (!vm.inactivityCheckingId) {
                vm.startCheckingIfSessionIsInactive();
            }
        }
    }

    vm.closeWarningModalAndSetInstanceToNull = function() {
        vm.warningModalInstance.close();
        vm.warningModalInstance = null;
    }
});