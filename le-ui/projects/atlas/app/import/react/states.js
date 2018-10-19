import About from "./components/About";
import Hello from "./components/Hello";
import GridContainer from '../templates/templates-grid-container';

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

export const hello = {
  name: "hello",
  parent: 'about',
  url: "/hello",
  views: {
    reactsub: Hello
  }
};

export const about = {
  name: "about",
  url: "/about",
  views: {
    reactmain: About
  }
};
