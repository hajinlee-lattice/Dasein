import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import { StateService } from "ui-router-ng2";

import { HttpClient } from './interceptor.service';
import { SessionService } from '../services/session.service';
import { StringUtility } from '../utilities/string.utility';
import { StringsUtility } from '../utilities/strings.utility';
import { StorageUtility } from '../utilities/storage.utility';

// Import RxJs required methods
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';
declare var $:any;
declare var CryptoJS:any;
declare var window:any;

@Injectable()
export class LoginService {

    constructor(
        private http: HttpClient,
        private sessionService: SessionService,
        private stringUtility: StringUtility,
        private stringsUtility: StringsUtility,
        private storageUtility: StorageUtility,
        private state: StateService
    ) { }

    public Login(username, password): Promise<any> {
        let passwordHash = CryptoJS.SHA256(password);

        return  this.http
                .post('/pls/login', { 
                    Username: username, 
                    Password: passwordHash.toString() 
                })
                .map(
                    (response: Response) => {
                        console.log('res',response);
                        var result = response["_body"];

                        result = JSON.parse(result);

                        if (result.errorCode == 'LEDP_18001') {
                            result.errorMessage = this.stringsUtility.getString('DEFAULT_LOGIN_ERROR_TEXT');
                        }

                        if (result) {
                            result.Result.UserName = username;

                            this.storageUtility.setTokenDocument(result.Uniqueness + "." + result.Randomness);
                            this.storageUtility.setLoginDocument(result.Result);
                        }
                        
                        return result;
                    },
                    (err) => {
                        console.log('err',err);
                        let result = {
                            Success: false,
                            errorMessage: this.stringsUtility.getString('LOGIN_UNKNOWN_ERROR')
                        };

                        return result;
                    }
                )
                .catch((err: Response) => {
                    console.log('catch',err);
                    let result = {
                        Success: false,
                        errorMessage: this.stringsUtility.getString('LOGIN_UNKNOWN_ERROR')
                    };
                    
                    return Observable.throw(err || 'backend server error');
                })
                .toPromise();
    }
    
    public Logout(): Promise<any> {
        return  this.http
                .get('/pls/logout')
                .map(
                    (response: Response) => {
                        var data = response["_body"];

                        data = JSON.parse(data);
                        
                        if (data != null && data.Success === true) {
                            this.storageUtility.clear();
                            this.stringsUtility.clearResourceStrings();

                            setTimeout(() => { 
                                window.location.href = '/ng2'; 
                            }, 1);
                        } else {
                            this.sessionService.HandleResponseErrors(data, status);
                        }

                        return data;
                    },
                    (err) => { }
                )
                .toPromise();
    }
    
    public GetSessionDocument(tenant): Promise<any> {
        return  this.http
                .post('/pls/attach', tenant)
                .map(
                    (response: Response) => {
                        var data = response["_body"];

                        data = JSON.parse(data);

                        if (data != null && data.Success === true) {
                            this.storageUtility.setSessionDocument(data.Result);
                            data.Result.User.Tenant = tenant;
                            this.storageUtility.setClientSession(data.Result.User)
                        }
                        if (data.Result.User.AccessLevel === null) {
                            this.sessionService.HandleResponseErrors(data, status);
                        }

                        return data;
                    },
                    (err) => {
                        this.sessionService.HandleResponseErrors({}, err.status);

                        return err;
                    }
                )
                .toPromise();
    }

    // If a user forgets their password, this will reset it and notify them
    public ResetPassword(username): Promise<any> {
        return  this.http
                .put('/pls/forgotpassword/', {
                    Username: username, 
                    Product: "Lead Prioritization", 
                    HostPort: this.getHostPort()
                })
                .map(
                    (response: Response) => {
                        let result = { Success: false };
                        let data = response["_body"];

                        data = JSON.parse(data);

                        if (data === true || data === 'true') {
                            result.Success = true;
                        } else {
                            this.sessionService.HandleResponseErrors(data, response.status);
                        }

                        return result;
                    },
                    (err) => {
                        var result = { Success: false, Error: err };
                        return result;
                    }
                )
                .toPromise();
    }
    
    public ChangePassword(oldPassword, newPassword, confirmNewPassword): Promise<any> {
        if (
            this.stringUtility.IsEmptyString(oldPassword) || 
            this.stringUtility.IsEmptyString(newPassword) || 
            this.stringUtility.IsEmptyString(confirmNewPassword)
        ) {
            return $.when(); // return an empty Promise via jQuery
        }

        let creds = {
            OldPassword : CryptoJS.SHA256(oldPassword).toString(),
            NewPassword : CryptoJS.SHA256(newPassword).toString()
        };

        let username = this.storageUtility.getLoginDocument().UserName;

        return  this.http
                .put('/pls/password/' + username + '/', creds)
                .map(
                    (response: Response) => {
                        let data = response["_body"];

                        data = JSON.parse(data);

                        let result = {
                            Success:    true,
                            Status:     response.status
                        };

                        if (!data.Success) {
                            result.Success = false;
                        }

                        return result;
                    },
                    (err) => {
                        let result = { 
                            Success: false, 
                            Status: err.status, 
                            Error: err 
                        };

                        return result;
                    }
                )
                .toPromise();
    }

    private getHostPort(): string {
        let host = window.location.host;
        let protocal = window.location.protocol;
        //let port = window.location.port;

        return protocal + "//" + host;
    }
    
}