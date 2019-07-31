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
    getSolutionInstanceByIdQuery(solutionInstanceId) {
      let q = `query {
          viewer {
            solutionInstances (criteria: {id: "${solutionInstanceId}"}) {
              edges {
                node {
                  id
                  name
                  enabled
                  created
                  authValues {
                    externalId
                    authId
                  }
                }
              }
            }
            authentications {
              edges {
                node {
                  id
                  name
                  customFields
                }
              }
            }
          }
        }`
        return { query: q };
    },
    updateSolutionInstanceQuery(solutionInstanceId, solutionInstanceName, authValues) {
      if (!authValues) {
        let q = `mutation {
            updateSolutionInstance(input: {solutionInstanceId: "${solutionInstanceId}",
                instanceName: "${solutionInstanceName}", enabled: true
            }) {
              solutionInstance {
                id
                name
                enabled
                created
              }
            }
          }`
          return { query: q };
        } else {
        let q = `mutation {
            updateSolutionInstance(input: {solutionInstanceId: "${solutionInstanceId}",
                instanceName: "${solutionInstanceName}", enabled: true, authValues: [{externalId: "${authValues.externalId}", authId: "${authValues.authId}"}]
            }) {
              solutionInstance {
                id
                name
                enabled
                created
              }
            }
          }`
          return { query: q };     
        }
        
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
    },
    /*
        EPHEMERAL API QUERIES
    */
    listMarketoStaticLists(authenticationId, programName) {
      if (!programName) { 
        return {
             auth_id: authenticationId,
             message:"list_static_lists",
             step_settings:{
                client_id:{
                   type:"jsonpath",
                   value:"$.auth.client_id"
                },
                client_secret:{
                   type:"jsonpath",
                   value:"$.auth.client_secret"
                },
                endpoint:{
                   type:"jsonpath",
                   value:"$.auth.endpoint"
                },
                batch_size:{
                   type:"integer",
                   value:300
                }
             }
          };
      } else {
          return {
               auth_id: authenticationId,
               message:"list_static_lists",
               step_settings:{
                  client_id:{
                    type:"jsonpath",
                    value:"$.auth.client_id"
                  },
                  client_secret:{
                    type:"jsonpath",
                    value:"$.auth.client_secret"
                  },
                  endpoint:{
                    type:"jsonpath",
                    value:"$.auth.endpoint"
                  },
                  batch_size:{
                    type:"integer",
                    value:300
                  },
                  program_names: {
                    type: "array",
                    value: [
                      {
                        type: "string",
                        value: programName
                      }
                    ]
                  }
               }
            };        
      }
    },
    getMarketoPrograms(authenticationId, date){
      if (!date) {
        return {
              auth_id: authenticationId,
              message: "list_programs",
              step_settings: {
                  client_id: {
                      type: "jsonpath",
                      value: "$.auth.client_id"
                  },
                  client_secret: {
                      type: "jsonpath",
                      value: "$.auth.client_secret"
                  },
                  endpoint: {
                      type: "jsonpath",
                      value: "$.auth.endpoint"
                  },
                  max_return: {
                      type: "integer",
                      value: 200
                  }
              }
          };
      } else {
        return {
              auth_id: authenticationId,
              message: "list_programs",
              step_settings: {
                  client_id: {
                      type: "jsonpath",
                      value: "$.auth.client_id"
                  },
                  client_secret: {
                      type: "jsonpath",
                      value: "$.auth.client_secret"
                  },
                  endpoint: {
                      type: "jsonpath",
                      value: "$.auth.endpoint"
                  },
                  max_return: {
                      type: "integer",
                      value: 200
                  },
                  earliest_updated_at: {
                      type: "string",
                      value: date
                  }
              }
          };
      }
    }


};
module.exports = Queries;