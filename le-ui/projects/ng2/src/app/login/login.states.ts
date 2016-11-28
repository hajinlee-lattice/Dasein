import { Ng2StateDeclaration } from "ui-router-ng2";

import { LoginComponent } from './login.component';
import { FormComponent } from './form/form.component';
import { TenantsComponent } from './tenants/tenants.component';
import { ForgotComponent } from './forgot/forgot.component';
import { UpdateComponent } from './update/update.component';
import { SuccessComponent } from './update/success/success.component';
import { TimedoutComponent } from '../shared/components/timedout/timedout.component';

import { StorageUtility } from "../shared/utilities/storage.utility";

export let LOGIN_STATES: Ng2StateDeclaration[] = [
    { 
        name: 'app.login.form', 
        url: '/form',
        views: {
            content: {
                component: FormComponent
            }
        }
    },{
        name: 'app.login.forgot', 
        url: '/forgot',
        views: {
            content: {
                component: ForgotComponent
            }
        }
    },{
        name: 'app.login.update', 
        url: '/update',
        views: {
            content: {
                component: UpdateComponent
            }
        }
    },{
        name: 'app.login.update.success', 
        url: '/success',
        views: {
            content: {
                component: SuccessComponent
            }
        }
    },{ 
        name: 'app.login.tenants', 
        url: '/tenants',
        resolve: [{
            token: 'LoginDocument', 
            deps: [StorageUtility],
            resolveFn: (storageUtility: StorageUtility) => storageUtility.getLoginDocument() || {}
        },{
            token: 'TenantList', 
            deps: ['LoginDocument'],
            resolveFn: (loginDocument) => loginDocument.Tenants || []
        }],
        views: {
            content: {
                component: TenantsComponent
            }
        }
    },{ 
        name: 'app.login.timedout', 
        url: '/timedout', 
        views: {
            modal: {
                component: TimedoutComponent
            }
        }
    }
];