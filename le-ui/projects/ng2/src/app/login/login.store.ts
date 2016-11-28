import { Injectable } from '@angular/core';
import { StorageUtility } from "../shared/utilities/storage.utility";
import { TimestampUtility } from '../shared/utilities/timestamp.utility';

declare var window:any;

@Injectable()
export class LoginStore {

    public login: any;

    constructor(
        private storageUtility: StorageUtility, 
        private timestampUtility: TimestampUtility
    ) {
        this.reset();
    }
    
    public get(): any {
        return this.login;
    }

    public set(LoginDocument, ClientSession): void {
        this.login.username = LoginDocument.UserName;
        this.login.expires = LoginDocument.PasswordLastModified;
        this.login.expireDays = Math.abs(this.timestampUtility.getDays(LoginDocument.PasswordLastModified) - 90);

        if (ClientSession && ClientSession.Tenant) {
            this.login.tenant = ClientSession.Tenant.DisplayName;
        }
    }

    public reset(): void {
        this.login = {
            username: '',
            expires: null,
            expireDays: 0,
            tenant: ''
        }
    }

    public redirectToLP(Tenant?) {
        if (!Tenant) {
            var ClientSession = this.storageUtility.getClientSession();
            var Tenant = ClientSession.Tenant;
        }

        var UIVersion = Tenant.UIVersion || "2.0";
        var pathMap = {
            "3.0": "/lp/",
            "2.0": "/lp2/"
        };

        var previousSession = this.storageUtility.getClientSession();

        return window.open(pathMap[UIVersion] || "/lp2", "_self");
    }
}