## PRE-REQUISITE PROGRAMS
You must have these programs installed:

1.  Ruby
    sudo apt-get install ruby-full

2.  SASS (needs Ruby)
    sudo su -c "gem install sass"

3.  NodeJS/NPM
    curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash -
    sudo apt-get install -y nodejs

4.  grunt-cli
    sudo npm install -g grunt-cli

Instructions are for Linux (Ubuntu) found via Google. Mac/Windows is similar, use Google


## Structure

-`bin`
    |
    |
    This folder contains the file for the configuration of the node environment for development. This is not used in production
-`conf`
    |
    |
    This folder contains configurations used in production mode
-`projects`
    |
    |
    All the UI projects are under this folder. The build process (grunt or wp) are going to compile the projects inside this folder
    |
    `common`
    This project can be built with grunt or webpack
        This projects contains all the components/services/widgets used from other projects
        This project also provides by means vendor.js the library that can be used
    `dante`
    This project is built only in grunt
    `insights`
    This project can be built with grunt or webpack
    `atlas`
    This project can be built with grunt or webpack
    This project contains the atlas code base
    `login`
    This project can be built with grunt or webpack
    This is the login app
    `tenatconsole`
    This project can be built only with grunt
    Project for tenants menagement
-server
    |
    |
    Node server
    `routes` 
    It contains the routes used to return the pages and the content during development and production.


## INSTALLATION INSTRUCTIONS

1.  svn checkout develop/ledp/le-ui
2.  type `npm install` in the le-ui folder to install all dependencies
    The post installation is going to run `npm run build`.

##DEVELOPMENT PROCESS
- USE OF STACK A
During this process the UI is going to call stack A for the API
Configuration for NODE_ENV ./bin/.enva
1. cd to the root of le-ui folder
2. `npm run wpdeva` ==> This command is going to call wp in watch mode for login, atlas, insight projects. It is also calling devsetup.js (bin folder) 
3. open browser on localhost:3001 ==> login app shown

- USE OF STACK B
During this process the UI is going to call stack B for the API
Configuration for NODE_ENV ./bin/.envb
1. cd to the root of le-ui folder
2. `npm run wpdeva` ==> This command is going to call wp in watch mode for login, atlas, insight projects. It is also calling devsetup.js (bin folder) 
3. open browser on localhost:3001 ==> login app shown

- BUILD PRODUCTION and RUN LOCAL
1. `npm run build`
2. `npm run nodea`
3. `htpp://localhost:3001` ==> min bundles are loaded in the login page
NOTE: The changes made in this mode are going to be reflected till `npm run build` command runs again. For dev mode use `npm run wpdeva` or `npm run wpdevb`.

##BUILD FOR PRODUCTION
`npm run build`
1. The grunt tasks runner is going to produce inside ./projects/common/assets the min.js files
2. The webpack bundler is going to produce inside login, atlas, insights a `dist` folder with html, js, other resorces

##package.json scripts
`install`: install all the node modules in all the projects and at the root level
`postinstall`: called by npm after `npm install`
`InstallDev`: Install all the node modules in all the projects
`dev`: run grun in dev mode stack A
`dev_lp`: 
`dev_login`
`dev_pd`: 
`dev_admin`:
`devb_admin`: 
`devb`:
`local`:
`local2`:
`local_admin`:
`newdev`:
`newdevb`:
`qa`:
`qadev`:
`prod`:
`proddev`:
`devallnowatch`:
`build`: build with grunt and once it is done build the webpack projects
`sass`: 
`sentry`:
`precommit`:
`lint`: 
`//`: 
`InstallCommon`: Install node modules in the `common` project
`InstallPD`: 
`InstallLP`: Install node modules in `atlas` project
`InstallLP2`:
`InstallInsights`: Install node modules in `insights` project
`InstallDante`: Install node modules in `dante` project
`InstallLogin`: Install node modules in `login` project
`InstallAdmin`: Install node modules in `tenantconsole` project
`InstallDevCommon`:
`InstallDevPD`: 
`InstallDevLP`: 
`InstallDevLP2`: 
`InstallDevInsights`:
`InstallDevDante`: 
`InstallDevLogin`: 
`InstallDevAdmin`: 
`commonbuild`: 
`pdbuild`: 
`lpbuild`: 
`lp2build`:
`insightsbuild`:
`dantebuild`: 
`loginbuild`: 
`adminbuild`: 
`commonsass`: 
`pdsass`: 
`lpsass`: 
`insightssass`: 
`dantesass`: 
`loginsass`: 
`adminless`: 
`commonsentry`: 
`pdsentry`: 
`lpsentry`: 
`insightssentry`: 
`dantesentry`: 
`loginsentry`: 
`adminsentry`: 
`lint_admin`: 
`wp-loginbuild`: build login project in production mode (dist folder generated inside login folder)
`wp-logindev`: build login project and start webpack in watch mode (if changes are made the bundle is regenerated)
`wp-lpdev`: build atlas project and start webpack in watch mode (if changes are made the bundle is regenerated)
`wp-lpprod`: build atlas project in production mode (dist folder generated inside atlas folder) 
`wp-insightsbuild`: build insights project in production mode (dist folder generated inside insights folder) 
`wp-insightsdev`: build insights project and start webpack in watch mode (if changes are made the bundle is regenerated)
`wpdeva`: build the projects and starts webpack in watch mode on stack A 
`wpdevb`: build the projects and starts webpack in watch mode on stack B
`wpdevlocal`: build the projects and starts webpack in watch mode on local
`wpqa`: build the projects and starts webpack in watch mode on qa environment
`wpbuild`: build all the webpack projects
`nodea`: start node server on stack A
`nodeb`: start node server on stack B 



#Visual Studio code setup debug mode
If you want to run VSC in debug mode create 
launch.json file with the following content

{
  // Use IntelliSense to learn about possible attributes.
  // Hover to view descriptions of existing attributes.
  // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
  "version": "0.2.0",
  "configurations": [
    {
      "type": "chrome",
      "request": "launch",
      "name": "Atlas Dev-Mode",
      "url": "http://localhost:3001", // or whatever port you use
      "webRoot": "${workspaceFoler}"
    }
  ]
}

#Debug Node server in Visual Studio

1.	Build  using npm run build
2.	Add this to the package.js 
   "debuga": "nodemon --nolazy --inspect-brk=9229 bin/devsetup.js env=qa"
3.	From terminal run npm run debuga
4.	In vs eneble the Auto attach (Command Pallette > Toggle Auto Attach: on)

And now you can put break point in vs

