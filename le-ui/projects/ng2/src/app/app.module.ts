import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { HttpModule } from '@angular/http';
import { HttpClient } from './shared/services/interceptor.service';


import { trace, Category, UIRouterModule, UIView } from "ui-router-ng2";
import { MAIN_STATES } from "./app.states";

import { AppComponent } from './app.component';
import { LpiComponent } from './lpi/lpi.component';
import { PdComponent } from './pd/pd.component';
import { LoginModule } from './login/login.module';
import { LoginComponent } from './login/login.component';
import { FormComponent } from './login/form/form.component';

import { StringUtility } from "./shared/utilities/string.utility";
import { StringsService } from "./shared/services/strings.service";
import { StringsUtility } from "./shared/utilities/strings.utility";
import { SessionService } from './shared/services/session.service';
import { LoginService } from './shared/services/login.service';
import { StorageUtility } from "./shared/utilities/storage.utility";
import { TimeoutUtility } from "./shared/utilities/timeout.utility";

@NgModule({
  declarations: [
    AppComponent,
    LpiComponent,
    PdComponent,
    LoginComponent
  ],
  imports: [
    BrowserModule,

    FormsModule,
    HttpModule,
    LoginModule,
    UIRouterModule.forRoot({
      states: MAIN_STATES,
      otherwise: { 
        state: 'app', 
        params: {} 
      },
      useHash: false
    }),
  ],
  providers: [
    StorageUtility,
    SessionService,
    StringsService, 
    StringsUtility,
    TimeoutUtility,
    StringUtility, 
    LoginService,
    HttpClient
  ],
  bootstrap: [ 
    UIView 
  ]
})
export class AppModule { }
