import GridContainer from './templates-grid-container';

let $state;
export const setAngularState = state => {
  console.log('The state is set NOW', state);
  $state = state;
};

export const getAngularState = () => {

  return $state;
};

export const templatelist = {
  name: "templatelist",
  url: "/templatelist",
  views: {
    reactmain: GridContainer
  }
};