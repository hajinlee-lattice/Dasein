import { axios, Observable, Subject } from '../../network.vendor';
import messageService from '../utilities/messaging-service';
import Response from './response';
import Error from './error';
import Observables from './observables';
import Message, {
    BANNER,
    ERROR,
} from '../utilities/message';

const httpName = 'http';
const observablesName = 'observables';
/**
 *
 * @param {*} axiosObj
 * @param {*} obj
 */
const setParams = (axiosObj, obj) => {
    if (obj) {
        Object.keys(obj).forEach(key => {
            axiosObj[key] = obj[key];
        });
    }
};
const init = () => {
    if (!window[httpName]) {
        console.log('init', window[httpName], typeof window[httpName]);
        window[httpName] = axios;
        window[observablesName] = new Observables();
    }
};

/**
 * http service
 */
const httpService = {
    constructor: () => {
        var http = window[httpName];
    },
    printObservables: () => {
        console.log('OBSERVABLES ',window[observablesName].getObservables());
    },
    /**
     * Set up headeer for all the requests
     */
    setUpHeader: headerObj => {
        console.log('setUpHeader', headerObj);
        setParams(http.defaults.headers.common, headerObj);
    },
    unsubscribeObservable: observer => {
        window[observablesName].removeObservable(observer.getName());
    },
    get: (url, observer, headers) => {
        let observable = Observable.create(obs => {
            http.get(url, { headers: headers ? headers : {} })
                .then(response => {
                    let resp = new Response(
                        response,
                        response.status,
                        response.statusText,
                        response.data
                    );
                    // if(response && response.data && response.data.UIAction){
                    messageService.sendMessage(
                        new Message(response, '', '', '', '')
                    );
                    // }
                    obs.next(resp);
                    obs.complete();
                })
                .catch(error => {
                    let respoError = new Error(
                        error.response.status,
                        error.response.statusText,
                        error.message
                    );
                    messageService.sendMessage(
                        new Message(
                            error.response,
                            BANNER,
                            ERROR,
                            respoError.getMsg(),
                            `${url} ${respoError.getFullMessage()}`
                        )
                    );
                    if (obs.error) {
                        obs.error(respoError);
                    }
                    obs.complete();
                });
        }).subscribe(observer);
        window[observablesName].addObservable(observer.getName(), observable);
    },

    post: (url, body, observer, headers) => {
        let observable = Observable.create(obs => {
            http.post(url, body, { headers: headers ? headers : {} })
                .then(response => {
                    let resp = new Response(
                        response,
                        response.status,
                        response.statusText,
                        response.data
                    );
                    // if(response && response.data && response.data.UIAction){
                    messageService.sendMessage(
                        new Message(response, '', '', '', '')
                    );
                    // }
                    obs.next(resp);
                    obs.complete();
                })
                .catch(error => {
                    let respoError = new Error(
                        error.response.status,
                        error.response.statusText,
                        error.message
                    );
                    messageService.sendMessage(
                        new Message(
                            error.response,
                            BANNER,
                            ERROR,
                            respoError.getMsg(),
                            `${url} ${respoError.getFullMessage()}`
                        )
                    );
                    if (obs.error) {
                        obs.error(respoError);
                    }
                    obs.complete();
                });
        }).subscribe(observer);
        window[observablesName].addObservable(observer.getName(), observable);
    },
    put: (url, body, observer, headers) => {
        let observable = Observable.create(obs => {
            http.put(url, body, { headers: headers ? headers : {} })
                .then(response => {
                    let resp = new Response(
                        response,
                        response.status,
                        response.statusText,
                        response.data
                    );
                    messageService.sendMessage(
                        new Message(response, '', '', '', '')
                    );
                    obs.next(resp);
                    obs.complete();
                })
                .catch(error => {
                    let respoError = new Error(
                        error.response.status,
                        error.response.statusText,
                        error.message
                    );
                    messageService.sendMessage(
                        new Message(
                            error.response,
                            BANNER,
                            ERROR,
                            respoError.getMsg(),
                            `${url} ${respoError.getFullMessage()}`
                        )
                    );
                    if (obs.error) {
                        obs.error(respoError);
                    }
                    obs.complete();
                });
        }).subscribe(observer);
        window[observablesName].addObservable(observer.getName(), observable);
    }
};
init();

export const HTTPFactory = () => {
    return {
        initHeader: headerObj => {
            httpService.setUpHeader(headerObj);
        },
        unsubscribeObservable: observer => {
            httpService.unsubscribeObservable(observer);
        },
        get: (url, observer) => {
            return httpService.get(url, observer);
        }
    };
};

export default httpService;