import { Component, OnInit } from '@angular/core';

import { StateService } from "ui-router-ng2";
import { LoginService } from '../../shared/services/login.service';
import { StringsUtility } from "../../shared/utilities/strings.utility";
import { StorageUtility } from '../../shared/utilities/storage.utility';
import { StringUtility } from '../../shared/utilities/string.utility';
import { TimestampUtility } from '../../shared/utilities/timestamp.utility';

declare var $:any;

@Component({
  selector: 'app-update',
  templateUrl: './update.component.html',
  styleUrls: ['./update.component.scss']
})
export class UpdateComponent implements OnInit {

    strings: StringsUtility;
    loginDocument: any;
    
    oldPassword: any = null;
    newPassword: any = null;
    confirmPassword: any = null;

    oldPasswordInputError: string = "";
    newPasswordInputError: string = "";
    confirmPasswordInputError: string = "";
    showPasswordError: boolean = false;
    isLoggedInWithTempPassword: boolean = false;
    isPasswordOlderThanNinetyDays: boolean = false;
    saveInProgess: boolean = false;

    validateErrorMessage: string;

    constructor(
        private loginService: LoginService,
        private storageUtility: StorageUtility,
        private stringUtility: StringUtility,
        private stringsUtility: StringsUtility,
        private timestampUtility: TimestampUtility,
        private stateService: StateService
    ) {
        this.strings = stringsUtility;
        this.validateErrorMessage = stringsUtility.getString("CHANGE_PASSWORD_HELP");
    }

    ngOnInit() {
        this.loginDocument = this.storageUtility.getLoginDocument() || {};
        this.isLoggedInWithTempPassword = this.loginDocument.MustChangePassword;
        this.isPasswordOlderThanNinetyDays = this.timestampUtility.checkLastModified(this.loginDocument.PasswordLastModified);
        
        //angular.element('body').addClass('update-password-body');

        $("#validateAlertError, #changePasswordSuccessAlert").hide();

        if (this.isPasswordOlderThanNinetyDays) {
            this.showPasswordError = true;
            this.validateErrorMessage = this.strings.getString("NINTY_DAY_OLD_PASSWORD");
        } else if (this.isLoggedInWithTempPassword) {
            this.showPasswordError = true;
            this.validateErrorMessage = this.strings.getString("MUST_CHANGE_TEMP_PASSWORD");
        }
    }


    private validatePassword(): boolean {
        $("#validateAlertError, #changePasswordSuccessAlert").fadeOut();
        this.oldPasswordInputError = this.stringUtility.IsEmptyString(this.oldPassword) ? "error" : "";
        this.newPasswordInputError = this.stringUtility.IsEmptyString(this.newPassword) ? "error" : "";
        this.confirmPasswordInputError = this.stringUtility.IsEmptyString(this.confirmPassword) ? "error" : "";

        if (this.oldPassword === "") {
            this.validateErrorMessage = this.strings.getString("LOGIN_PASSWORD_EMPTY_OLDPASSWORD");
            this.oldPasswordInputError = "error";
            return false;
        }

        if (this.oldPasswordInputError === "error" || this.newPasswordInputError === "error" ||
        this.confirmPasswordInputError === "error") {
            return false;
        }
        
        if (this.newPassword == this.oldPassword) {
            this.validateErrorMessage = this.strings.getString("LOGIN_PASSWORD_UPDATE_ERROR");
            this.newPasswordInputError = "error";
            this.confirmPasswordInputError = "error";
            return false;
        }

        if (this.newPassword !== this.confirmPassword) {
            this.validateErrorMessage = this.strings.getString("LOGIN_PASSWORD_MATCH_ERROR");
            this.newPasswordInputError = "error";
            this.confirmPasswordInputError = "error";
            return false;
        }

        if (!this.validPassword(this.newPassword).Valid) {
            this.newPasswordInputError = "error";
            this.validateErrorMessage = this.strings.getString("CHANGE_PASSWORD_HELP");
            return false;
        }
        
        return true;
    }

    private validPassword(password): any {
        var result = {
            Valid: true,
            ErrorMsg: null
        };

        if (password.length < 8) {
            result.Valid = false;
            result.ErrorMsg = this.strings.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var uppercase = /[A-Z]/;
        if (!uppercase.test(password)) {
            result.Valid = false;
            result.ErrorMsg = this.strings.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var lowercase = /[a-z]/;
        if (!lowercase.test(password)) {
            result.Valid = false;
            result.ErrorMsg = this.strings.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        var number = /[0-9]/;
        if (!number.test(password)) {
            result.Valid = false;
            result.ErrorMsg = this.strings.getString("CHANGE_PASSWORD_HELP");
            return result;
        }

        return result;
    }
    
    private clearChangePasswordField(): void {
        this.oldPassword = "";
        this.newPassword = "";
        this.confirmPassword = "";
    }
    
    public cancelAndLogoutClick($event): void {
        this.clearChangePasswordField();
        this.loginService.Logout();
    }
    
    public closeErrorClick($event): void {
        if ($event != null) {
            $event.preventDefault();
        }
        
        this.showPasswordError = false;
    }
    
    public updatePasswordClick() {
        if (this.saveInProgess) {
            return;
        }

        this.showPasswordError = false;

        var isValid = this.validatePassword();

        if (isValid) {
            this.saveInProgess = true;

            this.loginService.ChangePassword(this.oldPassword, this.newPassword, this.confirmPassword).then((result) => {
                this.saveInProgess = false;

                if (result.Success) {
                    $("#changePasswordSuccessAlert").fadeIn();
                    this.storageUtility.clear(false);
                    this.stateService.go('app.login.update.success');
                } else {
                    if (result.Status == 401) {
                        this.validateErrorMessage = this.strings.getString("CHANGE_PASSWORD_BAD_CREDS");
                    } else {
                        this.validateErrorMessage = this.strings.getString("CHANGE_PASSWORD_ERROR");
                    }
                    this.showPasswordError = true;
                }
            });
        } else {
            this.showPasswordError = true;
        }
    }

}
