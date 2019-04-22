import { store, injectAsyncReducer} from '../../app/store/index';
import {actions, reducer} from '../../widgets/banner/le-banner.redux';


class ReactMessagingService {
    constructor() {
        if (!ReactMessagingService.instance) {
            ReactMessagingService.instance = this;
            injectAsyncReducer(store, 'reactmessaging', reducer);
        }

        return ReactMessagingService.instance;
    }
    showBanner(response, options,callback) {
        let configuration = this.getConfiguration(response, options, callback);
        let type = configuration.type;
        switch(type){
            case 'error':
                actions.error(store, configuration);
            break;
            case 'info':
                actions.info(store, configuration);
            break;
            case 'success':
                actions.success(store, configuration);
            break;
            case 'warning':
                actions.warning(store, configuration);
            break;
        }
    }

    getConfiguration(response, options,callback){
        var payload = response.data,
        uiAction = payload.UIAction || {},
        method = (uiAction.status || "error").toLowerCase(),
        http_err = response.statusText,
        http_code = response.status,
        url = response.config
            ? response.config.url
            : '',
        title =
            typeof uiAction.title != "undefined"
                ? uiAction.title
                : http_code + ' "' + http_err + '" ' + url,
        message =
            uiAction.message ||
            payload.errorMsg ||
            payload.error_description,

        opts = {...{ title: title, message: message, type: method}, ...options };
        return opts;
    }
  
}

const instance = new ReactMessagingService();
Object.freeze(instance);

export default instance;
