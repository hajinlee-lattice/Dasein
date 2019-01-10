console.log('### INIT ###');

import { HTTPFactory } from '../../common/app/http/http-service';
import { MessagingFactory } from '../../common/app/utilities/messaging-service';
import StateHistory from './history.service.js';
import StateConfig from './routes.config.js';
import StateTransitions from './transitions.config.js';
import MainController from './app.controller.js';
import HTTP from './http.interceptor.js';
import Utils from './common.utils.js';

var mainApp = angular
    .module('mainApp', [
        'ngRoute',
        'ui.router',
        'ui.bootstrap',
        'angulartics',
        'angulartics.mixpanel',

        'common.modules',
        'common.modal',
        'common.banner',
        'common.notice',
        'common.exceptions',
        'common.attributes',
        'common.datacloud',

        'lp.navigation',
        'lp.widgets',
        'lp.jobs',
        //'lp.campaigns',
        //'lp.campaigns.models',
        'lp.segments',
        'lp.ratingsengine',
        //'lp.models.list',
        'lp.models.review',
        'lp.models.ratings',
        'lp.notes',
        'lp.playbook',
        'lp.importtemplates',
        'lp.import',
        'lp.delete',
        'lp.create.import',
        'lp.ssoconfig',
        'lp.sfdc',
        'lp.sfdc.credentials',
        'lp.apiconsole',
        'lp.configureattributes'
    ])
    .run(HTTP.AxiosAuthorization)
    .run(StateTransitions)
    .config(StateConfig)
    .config(Utils.Animation)
    .config(HTTP.InterceptorConfig)
    .config(HTTP.Config)
    .factory('authInterceptor', HTTP.Interceptor)
    .factory('LeMessaging', MessagingFactory)
    .factory('LeHTTP', HTTPFactory)
    .service('StateHistory', StateHistory)
    .filter('escape', Utils.EscapeFilter)
    .controller('MainController', MainController);
