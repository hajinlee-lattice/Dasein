import { axios, Observable, Subject } from "../../network.vendor";
import Response from "./response";
import Error from "./error";
import Observables from "./observables";

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
  unsubscribeObservable: observer =>{
      observables.removeObservable(observer.getName());
  },
  get: (url, observer) => {
    let observable = Observable.create(obs => {
      http
        .get(url)
        .then(response => {
          console.log("I am back from API", response);
          let resp = new Response(
            response.status,
            response.statusText,
            response.data
          );
          obs.next(resp);
          obs.complete();
        })
        .catch(error => {
            console.log(observables);
          let respoError = new Error(
            error.response.status,
            error.response.statusText,
            error.message
          );
          console.log("ERRRRRRRRRRR", error);
          obs.error(respoError);
          obs.complete();
        });
    }).subscribe(observer);
      observables.addObservable(observer.getName(),observable);
  },

  post: (url, body, observer) => {
    let observable = Observable.create(obs => {
      http
        .post(url, body)
        .then(response => {
          console.log("I am back from API", response);
          let resp = new Response(
            response.status,
            response.statusText,
            response.data
          );
          obs.next(resp);
        //   obs.complete();
        })
        .catch(error => {
          let respoError = new Error(
            error.response.status,
            error.response.statusText,
            error.message
          );
          console.log("ERRRRRRRRRRR", error);
          obs.error(respoError);
        //   obs.complete();
        });
    }).subscribe(observer);
      observables.addObservable(observer.getName(),observable);
  }
};
init();

export default httpService;
