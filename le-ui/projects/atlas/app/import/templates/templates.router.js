import './templates';
import './components/summary';

import {actions , reducer } from './multiple/multipletemplates.redux';
import './multiple/react-main.component';
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
            views: {
                'summary@': {
                    component: 'leSummaryComponent'
                },
                'main@': {
                    component: 'templatesComponent'
                }
            },
        })
        .state('home.multipletemplates', {
            url:'/multitemplates',
            params: {
                pageIcon: 'ico-analysis',
                pageTitle: 'Data Processing & Analysis'
            },
            onEnter: ($state, ReduxService) => {
                ReduxService.connect(
                    'multitemplates',
                    actions,
                    reducer,
                    $state.get('home.multipletemplates')
                )
            },
            onExit: function ($state) {
                console.log('multitemplates unsubscribe store');
                $state.get('home.multipletemplates').data.redux.unsubscribe();
            },
            resolve: {
                path: () => {
                    return 'templateslist';
                }
            },
            views: {
                'main@': {
                    component: 'reactMainComponent'
                }
            }
        });
});