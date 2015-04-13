# le-common.js Dev Documentation
 
le-common.js contains some utilities and front-end components that can be shared with multiple webapps in ledp project.
There are mainly two goals of this library: 

1.  DRY principle
2.  enforce the consistence of some logic, such as password requirement

## Prerequisites 

Node.js, npm, grunt-cli, ledp

The code assumes that the following libraries are loaded before loading le-common.js:

* jquery
* angular
* angular-sanitize
* angular-local-storage
* underscore

## Initialization
 
Check out the repository https://lesvn.lattice.local/svn/lattice/LEDP/branches/develop/ledp.
The codebase of le-common.js is located in the directory le-common/npm. Then run

    npm install
    grunt init

## Tests
 
In le-common/npm, run

    grunt unit
 
## Release
 
In le-common/npm, run
    
    grunt
    
This will create a le-common.js in release/0.0.1.