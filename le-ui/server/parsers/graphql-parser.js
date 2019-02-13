// export const trayAPI = '/tray';
// window['reactrouter'] =var router;
const GraphQLParser = {
    getUserInfo(data, edges){
        console.log('DATA', data);
        if(data.users && data.users.edges && data.users.edges.length > 0){
            var edges = data.users.edges;
            return new User(edges[0].node.name, edges[0].node.id, edges[0].node.externalUserId);
        }else{
            return null;
        }
    },
    getNewSolutionInstance(data, tagName){
        if(data && data.viewer){
            let url = data.viewr.solutions.edges
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

class SolutionInstance {
    constructor(tagName="", url=""){
        this.tagName = tagName;
        this.url = url;

    }

}