const Queries = {
    getUserQuery(userName) {
        let q = `query {
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

        return {
            query: q
        };
    },
    getCreateUserMutation(userName) {
        let q = `mutation {
            createExternalUser(input: {name: "${userName}", externalUserId: "${userName}"}) {
              userId
              clientMutationId
            }
          }`;
        return {
            query: q
        }
    },
    getSolutionsByTagQuery(tagName) {
        let q = `query {
                  viewer {
                    solutions (criteria: {tags: "${tagName}"}) {
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
                }`;
        return { query: q };
    },
    getAuthorizationCodeMutation(userId) {
        let q = `mutation {
                  generateAuthorizationCode(input: {userId: "${userId}"}) {
                    authorizationCode
                  }
                }`;
        return { query: q };
    },
    getAuthorizeUserMutation(userId) {
        let q = `mutation {
                  authorize(input: {userId: "${userId}"}) {
                    accessToken
                    clientMutationId
                  }
                }`;
        return { query: q };
    },
    getCreateSolutionInstanceMutation(solutionId, instanceName, authValues){
      if (!authValues) {
        let q = ` mutation {
                   createSolutionInstance(input: {
                       solutionId: "${solutionId}",
                       instanceName: "${instanceName}"
                   }) {
                     solutionInstance {
                       id
                       name
                       enabled
                     }
                   }
                 }`;
        return { query: q };
      } else {
        let q = ` mutation {
                   createSolutionInstance(input: {
                       solutionId: "${solutionId}",
                       instanceName: "${instanceName}"
                       authValues: [{ externalId: "${authValues.externalId}", authId: "${authValues.authId}" }]
                   }) {
                     solutionInstance {
                       id
                       name
                       enabled
                     }
                   }
                 }`;
        return { query: q };    
      }
    },
    getAuthentications() {
      let q = `query {
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
              }`;
      return { query: q };
    }


};
module.exports = Queries;