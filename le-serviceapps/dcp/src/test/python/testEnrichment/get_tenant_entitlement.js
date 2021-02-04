const newman = require('newman'),
      fs = require('fs');
	  
const fileName_1 = 'response.json';

newman.run({
  
    collection: require('./tenant_collection.json'),
	environment: require('./dev.json'),
    insecure: true,
    envvar: 'test=name',
    reporters: 'cli',
}).on('request', function (error, args) {
    if (error) {
        console.error(error);
    }
    else {
		fs.writeFile(fileName_1, args.response.stream, function (error) {
            if (error) { 
                console.error(error); 
            }
        });        
    }
});

