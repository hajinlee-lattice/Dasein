angular.module('lp.enrichment.leadenrichment')
.service('EnrichmentStore', function($q, EnrichmentService){
    var EnrichmentStore = this;
    this.enrichments = {
        attributes: [
        { id: 0, name: 'Firmographics' },
        { id: 1, name: 'Industry' },
        { id: 2, name: 'Technology' }
        ],
        total: {
            selected: 999,
            premium: 3,
        },
        enrichments: [
        {
            title: 'Wormwhole Phenomenon', 
            description: 'We\'re aquanited with the wormhole, but the wormhalf is the more remarkable of phenomenon. It\'s gross, but on a cosmic scale.',
            premium: true,
        },
        {
            title: 'My water glass brand is "bubba"',
            description: 'In American usage, "bubba" is a relationship nickname formed from "brother" and given to boys, especially eldest male siblings, to indicate their role in a family. For some boys and men, bubba is used so pervasively that it replaces the given name.',
            premium: true,
        },
        {
            title: 'Lorem ipsum',
            description: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse ultricies malesuada purus, vel rhoncus ipsum. Nullam in eros sed lorem mattis gravida. Nunc sollicitudin justo in auctor porttitor. Quisque luctus justo id feugiat fringilla. In hac habitasse platea dictumst. Curabitur vehicula elit sed risus sollicitudin, sed tempus lorem cursus. Cras feugiat, quam id ultricies ullamcorper, ligula metus vulputate libero, vitae rhoncus ligula ligula sit amet nisl. Fusce ut erat massa.',
            premium: false,
        },
        {
            title: 'Integer ante risus',
            description: 'Integer ante risus, egestas vel nisi ut, porta tincidunt nisi. Fusce sed ex sodales, sollicitudin libero vel, interdum libero. Cras pulvinar tortor quis lorem laoreet efficitur. Aenean non facilisis libero, nec pellentesque odio. Donec condimentum feugiat lectus, vitae bibendum libero. Nullam ac magna nec dolor pellentesque ultricies quis eget ligula. Quisque volutpat id leo in posuere. Donec lacinia sem felis, luctus tempor leo rutrum eget. Cras sit amet tellus efficitur, consequat augue quis, finibus nulla. Sed auctor nulla et risus malesuada congue. Fusce consequat dui lorem, gravida commodo arcu elementum et.',
            premium: false,
        },
        {
            title: 'Donec quam magna',
            description: 'Donec quam magna, lobortis et mi posuere, faucibus iaculis risus. Nunc placerat tellus nec eros auctor venenatis. Cras felis tortor, luctus nec imperdiet a, convallis sit amet urna. In massa nunc, dapibus quis iaculis at, posuere ut nulla. Mauris mattis ante id eros pharetra pellentesque. Maecenas luctus porttitor scelerisque. Quisque tincidunt quam lorem, vel blandit leo ullamcorper in. Vivamus vitae molestie nisl. Curabitur lorem risus, vulputate a enim in, rhoncus maximus nulla. Interdum et malesuada fames ac ante ipsum primis in faucibus. Aliquam ut consectetur eros. Suspendisse fringilla nisl a ornare mollis. Aliquam id lobortis libero. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Curabitur in ante nulla.',
            premium: false,
        }
        ]

    };
    //this.enrichments = null;

    this.get = function(){
        var deferred = $q.defer();
        if (this.enrichments) {
            deferred.resolve(this.enrichments);
        } else {
            EnrichmentService.getEnrichments().then(function(response){
                EnrichmentStore.set(response);
                deferred.resolve(response);
            });
        }
        return deferred.promise;
    }
    this.set = function(item){
        this.enrichments = item;
    }
})
.service('EnrichmentService', function($q, $http){
    this.getCategories = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/<some api>'
        }).then(function(response){
            deferred.resolve(response);
        });
        return deferred.promise;
    }

    this.getEnrichments = function(){
        var deferred = $q.defer();
        $http({
            method: 'get',
            url: '/pls/enrichment/lead'
        }).then(function(response){
            console.log('/pls/enrichment/lead - repsonse: ',response);
            deferred.resolve(response);
        });
        return deferred.promise;
    }
});
