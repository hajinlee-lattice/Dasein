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
    },
    getUserInfo(edges){
        if(edges && edges.length > 0){
            return new User(edges[0].node.name, edges[0].node.id, edges[0].node.externalUserId);
        }else{
            return new User();
        }

    }
};
export default ConnectorsService;

export class User{
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