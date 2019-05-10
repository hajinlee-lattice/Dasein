import './templates';
import './components/summary';

import {actions , reducer } from './multiple/multipletemplates.redux';
import '../../react/react-angular-main.component';
angular
.module('lp.importtemplates', [
    
    'le.import.templates',
    'le.summary',
    'lp.import'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.importtemplates', {
            url: '/templates',
            onEnter: function(ImportWizardStore){
                ImportWizardStore.clear();
            },
            params: {
                tenantName: { dynamic: true, value: '' },
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            resolve: {
                path: () => {
                    return 'templatelist';
                },
                ngservices: (TemplatesStore, ImportWizardStore) => {
                    let obj = {
                        TemplatesStore : TemplatesStore,
                        ImportWizardStore: ImportWizardStore
                    }
                    return obj;
                }
            },
            views: {
                'main@': {
                    component: 'reactAngularMainComponent'
                }
            },
        })
        .state('home.viewmappings', {
            url: '/mappings',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            resolve: {
                path: () => {
                    return 'fieldmappings';
                },
                ngservices: (ImportWizardStore) => {
                    let obj = {
                        ImportWizardStore: ImportWizardStore
                    }
                    return obj;
                }
            },
            views: {
                'main@': {
                    component: 'reactAngularMainComponent'
                }
            },
        })
        .state('home.multipletemplates', {
            url:'/multitemplates',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            resolve: {
                path: () => {
                    return 'templateslist';
                },
                ngservices: (ImportWizardStore) => {
                    let obj = {
                        ImportWizardStore: ImportWizardStore
                    }
                    return obj;
                }
            },
            views: {
                'main@': {
                    component: 'reactAngularMainComponent'
                }
            }
        });
});