import { Injectable } from '@angular/core';
import { StateService } from "ui-router-ng2";
import { LoginService } from '../services/login.service';
import { StorageUtility } from './storage.utility';
import { TimedoutComponent } from '../components/timedout/timedout.component';

declare var $:any;

@Injectable()
export class TimeoutUtility {

    TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS: number = 30 * 1000;
    TIME_INTERVAL_INACTIVITY_BEFORE_WARNING: number = 60 * 1000;  // 14.5 minutes
    TIME_INTERVAL_WARNING_BEFORE_LOGOUT: number = 30 * 1000;
    inactivityCheckingId: any = null;
    warningModalInstance: any = null;
    sessionExpired: any = false;
    state: StateService;

    constructor(
        private storageUtility: StorageUtility,
        private loginService: LoginService,
        private stateService: StateService
    ) { 
        this.state = stateService;
    }

    init() {
        this.startObservingUserActivtyThroughMouseAndKeyboard();
        this.startCheckingIfSessionIsInactive();
    }

    refreshPreviousSession(tenant) {
        this.loginService.GetSessionDocument(tenant).then(
            (data) => {
                if (data && data.Success === true) {
                    this.startObservingUserActivtyThroughMouseAndKeyboard();
                    this.startCheckingIfSessionIsInactive();
                }
            }
        );
    }

    startObservingUserActivtyThroughMouseAndKeyboard() {
        $('body').mousemove((event) => {
            if (!this.warningModalInstance) {
                this.refreshSessionLastActiveTimeStamp();
            }
        });

        $('body').keypress((event) => {
            if (!this.warningModalInstance) {
                this.refreshSessionLastActiveTimeStamp();
            }
        });
    }

    startCheckingIfSessionIsInactive() {
        this.refreshSessionLastActiveTimeStamp();

        this.inactivityCheckingId = setInterval(
            this.checkIfSessionIsInactiveEveryInterval.bind(this), 
            this.TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS
        );
    }

    checkIfSessionIsInactiveEveryInterval() {
        var ignoreStates = [
            'home.models.import','home.models.pmml','home.model.scoring',
            'home.models.import.job','home.models.pmml.job'
        ];
        
        console.log('<!> timeout check', this.state, this);

        if (ignoreStates.indexOf(this.state.current.name) >= 0) {
            return;
        }

        if (Date.now() - this.storageUtility.getSessionLastActiveTimestamp() >= this.TIME_INTERVAL_INACTIVITY_BEFORE_WARNING) {
            if (!this.warningModalInstance) {
                this.cancelCheckingIfSessionIsInactiveAndSetIdToNull();
                this.openWarningModal();
            }

            setTimeout(
                this.callWhenWarningModalExpires.bind(this), 
                this.TIME_INTERVAL_WARNING_BEFORE_LOGOUT
            );
        }
    }
    
    refreshSessionLastActiveTimeStamp() {
        this.storageUtility.setSessionLastActiveTimestamp(Date.now());
    }
    
    public hasSessionTimedOut() {
        return Date.now() - this.storageUtility.getSessionLastActiveTimestamp() >=
            this.TIME_INTERVAL_INACTIVITY_BEFORE_WARNING + this.TIME_INTERVAL_WARNING_BEFORE_LOGOUT;
    }
    
    openWarningModal() {
        this.stateService.go('app.login.timedout');
        this.warningModalInstance = true;
        /*
        this.modal.alert()
            .size('lg')
            .showClose(true)
            .title('Session Timeout Warning')
            .body(`Your session is about to timeout`)
            .open();
        this.warningModalInstance = $modal.open({
            animation: true,
            backdrop: true,
            scope: $rootScope,
            templateUrl: '/app/modules/SessionTimeout/WarningModal.html'
        });

        $rootScope.refreshSession() {
            this.closeWarningModalAndSetInstanceToNull();
            this.startCheckingIfSessionIsInactive();
        };
        */
    }
    
    cancelCheckingIfSessionIsInactiveAndSetIdToNull() {
        clearInterval(this.inactivityCheckingId);
        this.inactivityCheckingId = null;
    }

    stopObservingUserInteractionBasedOnMouseAndKeyboard() {
        $(document.body).off("mousemove");
        $(document.body).off("keypress");
    }
    
    callWhenWarningModalExpires() {
        if (this.hasSessionTimedOut()) {
            console.log('<!> modal has expired and so has session');
            this.sessionExpired = true;
            this.stopObservingUserInteractionBasedOnMouseAndKeyboard();

            this.loginService.Logout();
        } else {
            console.log('<!> modal has expired');
            if (this.warningModalInstance) {
                this.closeWarningModalAndSetInstanceToNull();
            }

            if (!this.inactivityCheckingId) {
                this.startCheckingIfSessionIsInactive();
            }
        }
    }

    closeWarningModalAndSetInstanceToNull() {
        console.log('<!> Close Warning Modal')
        //this.warningModalInstance.close();
        this.warningModalInstance = false;
    }
    
}
