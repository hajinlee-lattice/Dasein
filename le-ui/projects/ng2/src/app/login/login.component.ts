import { Component, Inject, OnInit } from '@angular/core';
import { LoginService } from '../shared/services/login.service';
import { LoginStore } from "./login.store";
import { StateService } from "ui-router-ng2";
import { StringsUtility } from '../shared/utilities/strings.utility';
import { TimeoutUtility } from '../shared/utilities/timeout.utility';
import { StorageUtility } from "../shared/utilities/storage.utility";
import { StringsService } from "../shared/services/strings.service";
import { SessionService } from '../shared/services/session.service';

declare var $:any;

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.scss']
})
export class LoginComponent implements OnInit {
    public login: any;
    public state: StateService;
    public strings: StringsUtility;
    public clientSession;
    public loginDocument;
    
    constructor(
        private loginStore: LoginStore,
        private loginService: LoginService,
        private sessionService: SessionService,
        private stateService: StateService,
        private stringsService: StringsService,
        private storageUtility: StorageUtility,
        private timeoutUtility: TimeoutUtility,
        private stringsUtility: StringsUtility
    ) {
        this.login = loginStore.get();
        this.state = stateService;
        this.strings = stringsUtility;

        stringsService.GetResourceStringsForLocale(undefined, true);
    }

    ngOnInit() {
        let clientSession = this.storageUtility.getClientSession() || {};
        let loginDocument = this.storageUtility.getLoginDocument() || {};
        let state = this.state;
        
        if (this.timeoutUtility.hasSessionTimedOut() && loginDocument.UserName) {
            return this.loginService.Logout();
        }

        this.loginStore.set(loginDocument, clientSession);
        
        switch(state.current.name) {
            case 'app.login.form': 
                if (loginDocument.UserName) {
                    state.go('app.login.tenants');
                }

                break;

            case 'app.login.tenants':
                if (!loginDocument.UserName) {
                    state.go('app.login.form');
                }

                break;
        }
        
        setTimeout(function() {
            $('body').addClass('initialized');
        }, 1);
    }

    public clickLogout(event?): void {
        if (event != null) {
            event.preventDefault();
        }
        
        this.loginStore.reset();
        this.loginService.Logout();
    }

    public clickModelList(): void {
        this.loginStore.redirectToLP();
    }
}