import { HTTPFactory } from '../../common/app/http/http-service';
import StateHistory from './history.service.js';
import StateConfig from './routes.config.js';
import StateTransitions from './transitions.config.js';
import MainController from './app.controller.js';
import HTTP from './http.interceptor.js';
import Utils from './common.utils.js';

angular
    .module('Atlas', [
        'ngRoute',
        'ui.router',
        'ui.bootstrap',
        'oc.lazyLoad',
        'angulartics',
        'angulartics.mixpanel',
        'common.modules',
        'common.modal',
        'common.banner',
        'common.notice',
        'common.exceptions',
        'common.attributes',
        'common.datacloud',
        'atlas.segmentation',
        'lp.navigation',
        'lp.widgets',
        'lp.jobs',
        'lp.segments',
        'lp.ratingsengine',
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
        'lp.configureattributes',
        'le.react.maincomponent',
        'le.connectors'
    ])
    .run(HTTP.AxiosAuthorization)
    .run(StateTransitions)
    .config(StateConfig)
    .config(Utils.Animation)
    .config(HTTP.InterceptorConfig)
    .config(HTTP.Config)
    .factory('authInterceptor', HTTP.Interceptor)
    .factory('LeHTTP', HTTPFactory)
    .service('StateHistory', StateHistory)
    .filter('escape', Utils.EscapeFilter)
    .controller('MainController', MainController);
