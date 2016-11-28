import { Ng2StateDeclaration, loadNgModule } from "ui-router-ng2";
import { AppComponent } from "./app.component";
import { LoginComponent } from './login/login.component';
import { LpiComponent } from './lpi/lpi.component';
import { PdComponent } from './pd/pd.component';

export let MAIN_STATES: Ng2StateDeclaration[] = [
    { 
        name: 'app', 
        redirectTo: 'app.login.form',
        component: AppComponent
    },{ 
        name: 'app.login', 
        url: '/login', 
        component: LoginComponent
    },{ 
        name: 'app.lpi', 
        url: '/lpi', 
        component: LpiComponent
    },{ 
        name: 'app.pd', 
        url: '/pd', 
        component: PdComponent
    }
];