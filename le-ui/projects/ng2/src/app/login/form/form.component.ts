import { Component, OnInit } from '@angular/core';
import { StringsUtility } from "../../shared/utilities/strings.utility";
import { StorageUtility } from '../../shared/utilities/storage.utility';
import { TimeoutUtility } from '../../shared/utilities/timeout.utility';
import { LoginService } from '../../shared/services/login.service';
import { StateService } from "ui-router-ng2";
declare var $:any;

@Component({
    selector: 'app-form',
    templateUrl: './form.component.html',
    styleUrls: ['./form.component.scss'],
    providers: [LoginService]
})
export class FormComponent implements OnInit {
    strings: StringsUtility;
    visible: boolean = false;
    loginMessage: boolean = null;
    showLoginError: boolean = false;
    loginInProgress: boolean = false;
    loginErrorMessage: boolean = null;
    showSuccessMessage: boolean = false;
    showForgotPassword: boolean = false;
    forgotPasswordUsername: string = "";
    forgotPasswordErrorMessage: string = "";
    successMessage: string = "";
    usernameInvalid: boolean = false;
    passwordInvalid: boolean = false;
    username: string = "";
    password: string = "";
    copyrightString: string;

    constructor(
        private loginService: LoginService,
        private timeoutUtility: TimeoutUtility,
        private storageUtility: StorageUtility,
        private stateService: StateService,
        private stringsUtility: StringsUtility
    ) {
        this.strings = stringsUtility;
        this.copyrightString = stringsUtility.getString('LOGIN_COPYRIGHT', [(new Date()).getFullYear()]);
    }

    ngOnInit(): void {
        this.visible = true;

        $('[autofocus]').focus();
    }

    public loginClick(): void {
        this.showLoginError = false;
        this.loginMessage = this.strings.getString("LOGIN_LOGGING_IN_MESSAGE");
        
        if (this.loginInProgress) {
            return;
        }

        this.usernameInvalid = this.username === "";
        this.passwordInvalid = this.password === "";                                                   

        if (this.usernameInvalid || this.passwordInvalid) {
            return;
        }

        this.loginInProgress = true;

        var self = this;

        this.loginService.Login(this.username, this.password).then((result: any) => {
            console.log('hhh',result);
            self.loginInProgress = false;
            self.loginMessage = null;

            if (result != null && result.Success === true) {
                this.timeoutUtility.refreshSessionLastActiveTimeStamp();
                self.stateService.go('app.login.tenants');
            } else {
                self.showLoginHeaderMessage(result);
                self.showLoginError = true;
            }
        });
    }

    private showLoginHeaderMessage(message): void {
        if (message == null) {
            return;
        }

        if (message.indexOf("Global Auth") > -1) {
            message = this.strings.getString("LOGIN_GLOBAL_AUTH_ERROR");
        }

        this.loginErrorMessage = message;
        this.showLoginError = true;
    }

}