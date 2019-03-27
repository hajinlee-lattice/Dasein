import ProfilesComponent from './profiles.component';

// let $state;
// export const setAngularState = state => {
//   console.log('The state is set NOW', state);
//   $state = state;
// };

// export const getAngularState = () => {

//   return $state;
// };

export const profiles = {

  name: "profiles",
  url: "/",
  views: {
    mainreact: ProfilesComponent
  },
  resolve: [{
    token: 'profiles',
    deps: ['$transition$'],
    resolveFn: (trans) => {
      return trans.params();
    }
  }]
};

export const profilesConnector = {

  name: "profilesconnector",
  url: "/:nameConnector",
  views: {
    mainreact: ProfilesComponent
  },
  resolve: [{
    token: 'profilesconnector',
    deps: ['$transition$'],
    resolveFn: (trans) => {
      let params = trans.params().nameConnector;
      console.log('PARAMETERS ', params);
      return trans.params();
    }
  }]
};