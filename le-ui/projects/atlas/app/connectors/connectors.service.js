export const trayAPI = '/tray';
// window['reactrouter'] =var router;
const ConnectorsService = {
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
    }
};
export default ConnectorsService;