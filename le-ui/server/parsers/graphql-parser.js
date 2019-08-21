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
    getSolutionInfo(data, edges){
        console.log('DATA', data);
        if(data.viewer && data.viewer.solutions && data.viewer.solutions.edges && data.viewer.solutions.edges.length > 0){
            var edges = data.viewer.solutions.edges;
            return new Solution(edges[0].node.id, edges[0].node.title);
        }else{
            return null;
        }
    },
    getAuthorizeInfo(data){
        if(data && data.authorize){
            return new AccessToken(data.authorize.accessToken);
        }else{
            return null;
        }
    },
    getAuthorizationCodeInfo(data){
        if(data && data.generateAuthorizationCode){
            return new AuthorizationCode(data.generateAuthorizationCode.authorizationCode);
        }else{
            return null;
        }
    },
    getSolutionInstance(data){
        if (data.viewer.solutionInstances && data.viewer.solutionInstances.edges[0] && data.viewer.solutionInstances.edges[0].node && data.viewer.authentications && data.viewer.authentications.edges) {
            var solutionInstance = data.viewer.solutionInstances.edges[0].node;
            var authentications = data.viewer.authentications.edges;
            return {solutionInstance: solutionInstance, authentications: authentications};
        }
    },
    getUpdateSolutionInstanceInfo(data) {
        if (data.updateSolutionInstance && data.updateSolutionInstance.solutionInstance) {
            return data.updateSolutionInstance.solutionInstance;
        } else {
            return null;
        }
    },
    getSolutionInstanceInfo(data, edges){
        if(data.createSolutionInstance && data.createSolutionInstance.solutionInstance){
            var solutionInstance = data.createSolutionInstance.solutionInstance;
            return new SolutionInstance(solutionInstance.id, solutionInstance.name, solutionInstance.enabled);
        }else{
            return null;
        }
    },
    getNewSolutionInstance(data, tagName){
        if(data && data.viewer){
            let url = data.viewer.solutions.edges
        }else{
            return null;
        }
    },
    getSolutionConfigurationInfo(solutionInstanceId, authorizationCode) {
        if (authorizationCode.code) {
            return new SolutionConfiguration(solutionInstanceId, authorizationCode.code);
        } else {
            return null;
        }
    },
    getAwsAuthenticationId(data) {
        if (data && data.viewer && data.viewer.authentications && data.viewer.authentications.edges) {
            var awsAuthentications = data.viewer.authentications.edges.filter(function(edge) {
                var authentication = edge.node;
                return authentication && authentication.service && authentication.service.name == "aws-s3";
            });
            return awsAuthentications[0].node.id;
        }
    },
    getUserDocument(user, accessToken) {

        return new UserDocument(user, accessToken);
    },
    /* Specific to Ephemeral API endpoints */
    getErrorMessage(body) {
        var errorMessage;
        if (body.response && body.response.body && body.response.body.errors && body.response.body.errors.length > 0) {
            errorMessage = body.response.body.errors[0].message;
        } else if (body.response && body.response.statusCode != 200 && body.message) {
            errorMessage = body.message;
        }
        return errorMessage || "";
    },
    getFacebookAudiences(body) {
        if (body && body.data && body.data.callConnector && body.data.callConnector.output) {
            var output = JSON.parse(body.data.callConnector.output);
            if (!output.data) {
                return [];
            }
            const audienceList = output.data.map(audience => {
                return new Audience(audience.id, audience.name);
            });
            return audienceList;
        } else {
            return body;
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

class AccessToken {
    constructor(token=""){
        this.token = token;
    }
}

class UserDocument {
    constructor(user="", accessToken=""){
        this.id = user.id;
        this.name = user.name;
        this.externalId = user.externalId;
        this.accessToken = accessToken.token;
    }
}

class AuthorizationCode {
    constructor(code=""){
        this.code = code;
    }
}

class Solution {
    constructor(id="", title=""){
        this.id = id;
        this.title = title;

    }
}

class SolutionInstance {
    constructor(solutionInstanceId="", name="", enabled=""){
        this.id = solutionInstanceId;
        this.name = name;
        this.enabled = enabled;
    }
}

class SolutionConfiguration {
    constructor(solutionInstanceId="", code=""){
        this.solutionInstanceId = solutionInstanceId;
        this.authorizationCode = code;

    }
}

class Audience {
    constructor(audienceId="", audienceName=""){
        this.id = audienceId;
        this.name = audienceName;

    }
}

