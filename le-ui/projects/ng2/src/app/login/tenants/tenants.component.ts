import { Component, Inject, OnInit } from '@angular/core';
import { StringsUtility } from '../../shared/utilities/strings.utility';
import { StorageUtility } from '../../shared/utilities/storage.utility';
import { TimestampUtility } from '../../shared/utilities/timestamp.utility';
import { LoginService } from '../../shared/services/login.service';
import { LoginStore } from "../login.store";
import { StateService } from "ui-router-ng2";
import { TimeoutUtility } from '../../shared/utilities/timeout.utility';

declare var $:any;

@Component({
  selector: 'app-tenants',
  templateUrl: './tenants.component.html',
  styleUrls: ['./tenants.component.scss']
})
export class TenantsComponent implements OnInit {
    
    strings: StringsUtility;
    tenantMap: any = {}; 
    selected: any = null;
    SortProperty: string = 'RegisteredTime';
    SortDirection: string = '-';
    SearchValue: string = '';
    version: string = '3.0';
    isLoggedInWithTempPassword: boolean = false;
    isPasswordOlderThanNinetyDays: boolean;//TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(this.loginDocument.PasswordLastModified);
    visible: boolean = true;
    deactivated: boolean = false;
    initialize: boolean = false;
    tenantErrorMessage: string;
    showTenantError: boolean = false;
    timestamp: TimestampUtility;

    constructor(
        @Inject("TenantList") public tenantList,
        @Inject("LoginDocument") public loginDocument,
        private loginStore: LoginStore,
        private loginService: LoginService,
        private stringsUtility: StringsUtility,
        private storageUtility: StorageUtility,
        private timestampUtility: TimestampUtility,
        private timeoutUtility: TimeoutUtility,
        private stateService: StateService
    ) { 
        this.strings = stringsUtility;
        this.isLoggedInWithTempPassword = this.loginDocument.MustChangePassword;
        this.timestamp = timestampUtility;
    }

    ngOnInit(): void {
        let ClientSession = this.storageUtility.getClientSession();
        let loginDocument = this.storageUtility.getLoginDocument() || {};
        
        this.isPasswordOlderThanNinetyDays = this.timestamp.checkLastModified(this.loginDocument.PasswordLastModified);
        this.loginStore.set(this.loginDocument, ClientSession);
        
        if (this.isLoggedInWithTempPassword || this.isPasswordOlderThanNinetyDays) {
            this.stateService.go('app.login.update');
            return;
        }
        
        if (this.tenantList.length == 1) {
            this.select(this.tenantList[0]);
            return;
        }
        
        if (this.timeoutUtility.hasSessionTimedOut() && loginDocument.UserName) {
            this.loginService.Logout();
            return;
        }

        if (this.tenantList == null || this.tenantList.length === 0) {
            if (this.loginDocument && !this.loginStore.login.username) {
                this.showError(this.strings.getString("LOGIN_EXPIRED_AUTHENTICATION_CREDENTIALS"));
            } else {
                this.showError(this.strings.getString("NO_TENANT_MESSAGE"));
            }
            return;
        }

        var self = this;

        this.tenantList.forEach(function(tenant) {
            self.tenantMap[tenant.Identifier] = tenant;
        });

        this.timeoutUtility.init();

        this.initialize = true;

        $(document.body).click(function(event) { this.focus(); });

        this.focus();
    }

    public select(tenant): void {
        this.deactivated = true;
        this.selected = tenant;

        this.loginService.GetSessionDocument(tenant).then((data: any) => {
            if (data != null && data.Success === true) {
                this.loginStore.redirectToLP(tenant);
            } else {
                this.deactivated = false;
                this.selected = null;
                this.showError(this.strings.getString("TENANT_SELECTION_FORM_ERROR"));
            }
        });
    }

    public sort(value): void {
        if (this.SortProperty == value) {
            this.SortDirection = (this.SortDirection == '' ? '-' : '');
        } else {
            this.SortDirection = '';
        }

        this.SortProperty = value;
    }

    public toggle(): void {
        this.visible = !this.visible
        
        this.SearchValue = '';

        this.focus();
    }

    public focus(): void {
        setTimeout(function() {
            $('[autofocus]').focus();
        }, 100);
    }

    public keyDown($event): void {
        // convert html collection to array
        var all = [].slice.call($('.tenant-list-item'));
        var selected = [].slice.call($('.tenant-list-item.active,.tenant-list-item:hover'));
        let n:number;

        switch ($event.keyCode) {
            case 38: // up
                if (selected.length == 0) {
                    $(all[0]).addClass('active');
                } else {
                    var index = all.indexOf(selected[0]);
                    n = index == 0 ? all.length - 1 : index - 1;
                }

                break;
            case 40: // down
                if (selected.length == 0) {
                    $(all[0]).addClass('active');
                } else {
                    var index = all.indexOf(selected[0]);
                    n = index + 1 >= all.length ? 0 : index + 1;
                }

                break;
            case 13: // enter
                if (selected && selected.length > 0) {
                    var tenant = this.tenantMap[selected[0].id];
                    console.log('enter', tenant, selected);
                    if (tenant) {
                        this.select(tenant);
                    }
                }

                break;
        }
        
        if (typeof n == 'number' && n > -1) {
            $(all[n]).addClass('active');
            $(all[index]).removeClass('active');
        }
    }

    public hover(): void {
        $('.tenant-list-item.active').removeClass('active');
    }

    public showError(message): void {
        if (message == null) {
            return;
        }

        if (message.indexOf("Global Auth") > -1) {
            message = this.strings.getString("LOGIN_GLOBAL_AUTH_ERROR");
        }

        this.tenantErrorMessage = message;
        this.showTenantError = true;
    }

}
