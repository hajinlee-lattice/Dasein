import httpService from "../../../common/app/http/http-service";
import { Observable } from "../../../common/network.vendor";
import Observer from "../../../common/app/http/observer";

const URLS = { trayUrl: "/tray" };
// const temlpateUrl = "/pls/cdl/s3import/template";
const ExternalIntegrationService = () => {
  return new ExternalIntegrationServiceClass();
};
export default ExternalIntegrationService;

export const MARKETO_TAG = "MARKETO";

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
  },
  authentications: {
    query:
      `query {
        viewer {
          authentications {
            edges {
              node {
                id
                name
                service {
                  id,
                  name,
                  icon,
                  title,
                  version
                }
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
    let ok = new Observer(response => {
        httpService.unsubscribeObservable(ok);
        observer.next(response);
    }, (error) =>{
        httpService.unsubscribeObservable(ok);
        observer.error(error);
    });
    return ok;
  }

  /*
    MASTER token
  */
  getUsers(observer) {
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, QUERIES.users, ok);
  }

  /*
    MASTER token
    id: tray ==> for api
    name: dropbox
  */
  getUser(userName, observer) {
    let query = {
      query: `query {
        users (criteria: {name: "${userName}"}) {
          edges {
            node {
              name
              id
              externalUserId
            }
          }
        }
      }`
    };
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, query, ok);
  }

  /*
    MASTER token
  */
  getSolutions(observer) {
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, QUERIES.solutions, ok);
  }

  /**
   * 
   *  Create Process:
   *  1. Get solutions by tag = Marketo
   *      => node.id ==> solution id
   *  2. Create instance based on the solution id 1. ==> solution instance id
   *  3. Get generateAuthorizationCode ==> tocken used in iframe
   *  3. Create the popup iframe
   */

  /*
    MASTER token
    Filter by tag (e.g. Marketo, Salesforce, etc.)
  */
  getSolutionsByTag(tag, observer) {
    let query = {
      query:
        `query {
            viewer {
              solutions (criteria: {tags: "[${tag}]"}) {
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
    };
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, QUERIES.solutions, ok);
  }

  /*
    USER token
  */
  getSolutionInstances(userAccessToken, observer) {
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, QUERIES.solutionInstances, ok, userAccessToken ? {UserAccessToken: userAccessToken} : {});
  }

  /*
    USER token
    https://tray.io/docs/embedded/processes-to-be-managed/importing-auths.html
  */
  getAuthentications(userAccessToken, observer) {
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, QUERIES.authentications, ok, userAccessToken ? {UserAccessToken: userAccessToken} : {});
  }


  /*
    ----------
    MUTATIONS
    ----------
  */


  /*
    MASTER token
  */
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
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, mutation, ok);

  }

  /*
    MASTER token
  */
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
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, mutation, ok);
  }

  /*
    MASTER token
    externalUserId and name should be dropbox id
  */
  createExternalUser(userName, observer) {
    let mutation = {
      query:
        `mutation {
          createExternalUser(input: {name: "${userName}", externalUserId: "${userName}"}) {
            authorizationCode
            clientMutationId
          }
        }`
    };
    let ok = this.constructObserver(observer);
    httpService.post(URLS.trayUrl, mutation, ok);
  }


}
