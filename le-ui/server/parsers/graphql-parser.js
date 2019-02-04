// export const trayAPI = '/tray';
// window['reactrouter'] =var router;
const GraphQLParser = {
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
    getUserInfo(data, edges){
        console.log('DATA', data);
        if(data.users && data.users.edges && data.users.edges.length > 0){
            var edges = data.users.edges;
            return new User(edges[0].node.name, edges[0].node.id, edges[0].node.externalUserId);
        }else{
            return null;
        }

    }
};
module.exports = GraphQLParser;

class User{
    constructor(name='', id='', externalId=''){
        this.name = name,
        this.id = id;
        this.externalId = externalId;
    }
    getName(){
        return this.name;
    }
     getId() {
        return this.id;
    }
    getExternalId() {
        return this.externalId;
    }
    
}