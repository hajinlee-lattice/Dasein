import { axios, Observable, Subject } from "../../network.vendor";
import messageService from "../utilities/messaging-service";
import Response from "./response";
import Error from "./error";
import Observables from "./observables";
import Message, {
  MODAL,
  BANNER,
  NOTIFICATION,
  ERROR,
  INFO,
  WARNING
} from "../utilities/message";

let http;

let observables;

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
  if (!http) {
    http = axios;
    observables = new Observables();
  }
};

/**
 * http service
 */
const httpService = {
  /**
   * Set up headeer for all the requests
   */
  setUpHeader: headerObj => {
    setParams(http.defaults.headers.common, headerObj);
  },
  unsubscribeObservable: observer => {
    observables.removeObservable(observer.getName());
  },
  get: (url, observer, headers) => {
    let observable = Observable.create(obs => {
      http
        .get(url, { headers: headers ? headers : {}})
        .then(response => {
          let resp = new Response(
            response,
            response.status,
            response.statusText,
            response.data
          );
          // if(response && response.data && response.data.UIAction){
          messageService.sendMessage(new Message(response, "", "", "", ""));
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
    observables.addObservable(observer.getName(), observable);
  },

  post: (url, body, observer) => {
    let observable = Observable.create(obs => {
      http
        .post(url, body)
        .then(response => {
          let resp = new Response(
            response,
            response.status,
            response.statusText,
            response.data
          );
          // if(response && response.data && response.data.UIAction){
          messageService.sendMessage(new Message(response, "", "", "", ""));
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
    observables.addObservable(observer.getName(), observable);
  },
  put: (url, body, observer) => {
    let observable = Observable.create(obs => {
      http
        .put(url, body)
        .then(response => {
          let resp = new Response(
            response,
            response.status,
            response.statusText,
            response.data
          );
          messageService.sendMessage(new Message(response, "", "", "", ""));
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
    observables.addObservable(observer.getName(), observable);
  }
};
init();

export default httpService;
