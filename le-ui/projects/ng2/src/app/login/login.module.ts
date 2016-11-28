import { NgModule } from "@angular/core";
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { UIRouterModule } from "ui-router-ng2";
import { LOGIN_STATES } from "./login.states";

import { ModalModule } from 'angular2-modal';
import { BootstrapModalModule } from 'angular2-modal/plugins/bootstrap';

import { StringsService } from "../shared/services/strings.service";
import { StringsUtility } from "../shared/utilities/strings.utility";
import { TimestampUtility } from '../shared/utilities/timestamp.utility';
import { LoginStore } from "./login.store";

import { FormComponent } from './form/form.component';
import { TenantsComponent } from './tenants/tenants.component';
import { ForgotComponent } from './forgot/forgot.component';

import { OrderbyPipe } from '../shared/pipes/orderby.pipe';
import { FilterPipe } from '../shared/pipes/filter.pipe';
import { UpdateComponent } from './update/update.component';
import { SuccessComponent } from './update/success/success.component';
import { TimedoutComponent } from '../shared/components/timedout/timedout.component';

@NgModule({
    declarations: [
        FormComponent,
        TenantsComponent,
        OrderbyPipe,
        FilterPipe,
        ForgotComponent,
        UpdateComponent,
        SuccessComponent,
        TimedoutComponent
    ],
    imports: [
        BrowserModule,
        ModalModule.forRoot(),
        BootstrapModalModule,
        FormsModule,
        HttpModule,
        UIRouterModule.forChild({ 
            states: LOGIN_STATES
        })
    ],
    providers: [
        StringsService,
        StringsUtility,
        TimestampUtility,
        LoginStore
    ],
    entryComponents: [
        TimedoutComponent
    ]
})
export class LoginModule { }