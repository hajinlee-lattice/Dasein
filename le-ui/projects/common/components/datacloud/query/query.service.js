angular.module('common.datacloud.queryservice',[
])
.service('QueryStore', function($filter, QueryService) {
    function Account (companyName, website, city, state, country, salesforceID, marketoID, score) {
        this.CompanyName = companyName;
        this.WebSite = website;
        this.City = city;
        this.State = state;
        this.Country = country;
        this.SalesforceID = salesforceID;
        this.MarketoID = marketoID;
        this.Score = score;
    };

    function Contact (firstName, lastName, companyName, email, score) {
        this.FirstName = firstName;
        this.LastName = lastName;
        this.CompanyName = companyName;
        this.Email = email;
        this.Score = score;
    };

    var accountColumns = [
        {key: 'CompanyName', displayName: 'Company Name'},
        {key: 'WebSite', displayName: 'Web Address'},
        {key: 'City', displayName: 'City'},
        {key: 'State', displayName: 'State'},
        {key: 'Country', displayName: 'Country'},
        {key: 'SalesforceID', displayName: 'Salesforce ID'},
        {key: 'MarketoID', displayName: 'Marketo ID'},
        {key: 'Score', displayName: 'Score'}
    ];

    var contactColumns = [
        {key: 'FirstName', displayName: 'First Name'},
        {key: 'LastName', displayName: 'Last Name'},
        {key: 'CompanyName', displayName: 'Company Name'},
        {key: 'Email', displayName: 'Email'},
        {key: 'Score', displayName: 'Score'}
    ];

    var accounts = [],
        contacts = [];

    for (var i = 0; i < 88; i++) {
        accounts.push(new Account(
            'foo bar', 'www.' + 'foo' + '.com', 'foo', 'CA', 'USA', '001baz', 'baz', Math.floor(Math.random()*50 + 50)
        ));
    }

    for (var i = 0; i < 88; i++) {
        contacts.push(new Contact(
            'foo', 'bar', 'foobar', 'foo@bar.com', Math.floor(Math.random()*50 + 50)
        ));
    }

    this.getAccountColumns = function() {
        return accountColumns;
    };

    this.getContactColumns = function() {
        return contactColumns;
    };

    this.getPage = function(context, offset, maximum, query, sortBy, sortDesc) {
        switch (context) {
            case 'contacts': return this.getContacts(offset, maximum, query, sortBy, sortDesc);
            case 'accounts': return this.getAccounts(offset, maximum, query, sortBy, sortDesc);
        }
    };

    this.getAccounts = function(offset, maximum, query, sortBy, sortDesc) {
        var matched = query ? $filter('filter')(accounts, query) : accounts;
        return $filter('orderBy')(matched, sortBy, sortDesc).slice(offset, offset + maximum);

    };

    this.getContacts = function(offset, maximum, query, sortBy, sortDesc) {
        var matched = query ? $filter('filter')(contacts, query) : contacts;
        return $filter('orderBy')(matched, sortBy, sortDesc).slice(offset, offset + maximum);
    };

    this.getCount = function(context, query) {
        switch (context) {
            case 'contacts': return this.getContactCount(query);
            case 'accounts': return this.getAccountCount(query);
        }
    };

    this.getAccountCount = function(query) {
        var matched = query ? $filter('filter')(accounts, query) : accounts;
        return matched.length;
    };

    this.getContactCount = function (query) {
        var matched = query ? $filter('filter')(contacts, query) : contacts;
        return contacts.length;
    };

    var restrictions = {
        all: [],
        any: []
    };

    this.getRestrictions = function() {
        // mutable, to rollback, fetch from server
        return restrictions;
    };

    this.loadRestrictions = function() {
        for ( var i = 0; i < 3; i++) {
            var attr = {};
            attr.category = 'foo 321 567 ' + i;
            attr.subcategory = 'bar lmnop asdf ' + i;
            attr.buckets = [];

            for (var j = 0, l = Math.random() * 3 + 1 ; j < l; j++) {
                attr.buckets.push({
                    value: (Math.random() * 10*j + 10*j).toFixed(),
                    leads: (Math.random() * 5 + 1).toFixed(2) + '%',
                    lift: (Math.random() * 6).toFixed(1) + 'x'
                });
            }
            restrictions.all.push(attr);
        }

        for ( var i = 0; i < 3; i++) {
            var attr = {};
            attr.category = 'foo abc 123 ' + i;
            attr.subcategory = 'bar xyz 999 ' + i;
            attr.buckets = [];

            for (var j = 0, l = Math.random() * 3 + 1 ; j < l; j++) {
                attr.buckets.push({
                    value: (Math.random() * 10 *j + 10*j ).toFixed(),
                    leads: (Math.random() * 5 + 1).toFixed(2) + '%',
                    lift: (Math.random() * 6).toFixed(1) + 'x'
                });
            }
            restrictions.any.push(attr);
        }
    };

    this.addRestriction = function() {
        // append to all (default)
    };

    this.removeRestriction = function() {
        // search and remove from all or any
    };
})
.service('QueryService', function($http, $q) {
    /*
    POST /accounts/count
    POST /accounts/count/restriction
    POST /accounts/data

    POST /metadatasegments
    GET /metadatasegments/all
    GET /metadatasegments/name/{segmentName}
    DELETE /metadatasegments/{segmentName}
    */
});
