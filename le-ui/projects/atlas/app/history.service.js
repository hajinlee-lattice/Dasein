export default function() {
    'ngInject';

    this.states = {
        from: [],
        fromParams: [],
        to: [],
        toParams: []
    };

    this.lastTo = function() {
        return this.states.to[this.states.to.length - 1];
    };

    this.lastToParams = function() {
        return this.states.toParams[this.states.toParams.length - 1];
    };

    this.lastFrom = function() {
        return this.states.from[this.states.from.length - 1];
    };

    this.lastFromParams = function() {
        return this.states.fromParams[this.states.fromParams.length - 1];
    };

    this.isTo = function(name) {
        return this.lastTo().name == name;
    };

    this.isFrom = function(name) {
        return this.lastFrom().name == name;
    };

    this.setTo = function(state, params) {
        this.states.to.push(state);
        this.states.toParams.push(params);
    };

    this.setFrom = function(state, params) {
        this.states.from.push(state);
        this.states.fromParams.push(params);
    };
}
