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
    getCreateUserQuery(userName) {
        let q = `mutation {
            createExternalUser(input: {name: "${userName}", externalUserId: "${userName}"}) {
              authorizationCode
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
                    solutions (criteria: {tags: "[${tagName}]"}) {
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

    }

};
module.exports = Queries;