import { Component, OnInit } from '@angular/core';
import { LoginService } from '../../shared/services/login.service';
import { StringsUtility } from "../../shared/utilities/strings.utility";
import { StateService } from "ui-router-ng2";

@Component({
  selector: 'app-forgot',
  templateUrl: './forgot.component.html',
  styleUrls: ['./forgot.component.scss']
})
export class ForgotComponent implements OnInit {

    strings: StringsUtility;
    forgotPasswordErrorMessage: string = "";
    forgotPasswordUsername: string = "";
    resetPasswordSuccess: boolean = false;
    showForgotPasswordError: boolean = false;
    forgotPasswordUsernameInvalid: boolean = false;
    visible: boolean = true;

    constructor(
        private loginService: LoginService,
        private stateService: StateService,
        private stringsUtility: StringsUtility
    ) {
        this.strings = stringsUtility;
    }

    ngOnInit() { }

    public cancelForgotPasswordClick($event): void {
        if ($event != null) {
            $event.preventDefault();
        }
        
        this.stateService.go('app.login.form');
    }

    public validateEmail(string): boolean {
        if (!string) {
            return false;
        }

        var at = string.indexOf('@'),
            dot = string.lastIndexOf('.');

        if (at < 1 || dot - at < 2) {
            return false;
        } else {
            return true;
        }
    }

    public forgotPasswordOkClick(forgotPasswordUsername): void {
        var forgotPasswordUsername = forgotPasswordUsername || this.forgotPasswordUsername;

        this.resetPasswordSuccess = false;
        this.showForgotPasswordError = false;
        this.forgotPasswordUsernameInvalid = !this.validateEmail(forgotPasswordUsername);

        if (this.forgotPasswordUsernameInvalid) {
            return;
        }

        this.loginService.ResetPassword(forgotPasswordUsername).then((result) => {
            if (result == null) {
                return;
            }
            if (result.Success === true) {
                this.resetPasswordSuccess = true;
            } else {
                this.showForgotPasswordError = true;

                if (result.Error.errorCode == 'LEDP_18018') {
                    this.forgotPasswordUsernameInvalid = true;
                    this.forgotPasswordErrorMessage = this.strings.getString('RESET_PASSWORD_USERNAME_INVALID');
                } else {
                    this.forgotPasswordUsernameInvalid = false;
                    this.forgotPasswordErrorMessage = this.strings.getString('RESET_PASSWORD_FAIL');
                }
            }
        });
    }

}
