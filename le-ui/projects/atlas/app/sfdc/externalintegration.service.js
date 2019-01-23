import httpService from "../../../common/app/http/http-service";
import { Observable } from "../../../common/network.vendor";
import Observer from "../../../common/app/http/observer";

const URLS = { trayUrl: "/tray" };
// const temlpateUrl = "/pls/cdl/s3import/template";
const ExternalIntegrationService = () => {
  return new ExternalIntegrationServiceClass();
};
export default ExternalIntegrationService;

const QUERIES = {
  users: {
    query: `query {
      users {
        edges {
          node {
            name
            id
            externalUserId
          }
        }
      }
    }`
  },
  solutions: {
    query:
      `query {
          viewer {
            solutions {
              edges {
                node {
                  id
                  title
                  description
                  tags
                  customFields {
                    key
                    value
                  }
                }
                cursor
              }
              pageInfo {
                endCursor
                hasNextPage
              }
            }
          }
        }`
  },
  solutionInstances: {
    query:
      `query {
        viewer {
          solutionInstances {
            edges {
              node {
                id
                name
                enabled
                created
              }
            }
          }
        }
      }`
  }
}

class ExternalIntegrationServiceClass {
  constructor() {}

  constructObserver(observer) {
    return new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
  }

  getUsers(observer) {
    // let query = ;
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.post(URLS.trayUrl, QUERIES.users, ok);
  }

  getSolutions(observer) {
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.post(URLS.trayUrl, QUERIES.solutions, ok);
  }

  getSolutionInstances(userAccessToken, observer) {
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.post(URLS.trayUrl, QUERIES.solutionInstances, ok, userAccessToken ? {UserAccessToken: userAccessToken} : {});
  }


  authorize(userId, observer) {
    let mutation = {
      query:
        `mutation {
          authorize(input: {userId: "${userId}"}) {
            accessToken
            clientMutationId
          }
        }`
    };
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.post(URLS.trayUrl, mutation, ok);

  }

  generateAuthorizationCode(userId, observer) {
    let mutation = {
      query:
        `mutation {
          generateAuthorizationCode(input: {userId: "${userId}"}) {
            authorizationCode
            clientMutationId
          }
        }`
    };
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    httpService.post(URLS.trayUrl, mutation, ok);
  }


}
