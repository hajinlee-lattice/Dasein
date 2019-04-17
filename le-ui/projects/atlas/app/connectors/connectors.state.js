import ConnectionsComponent from './connections.component';

const connectorsList = {
  parent: 'home',
  name: "connectorslist",
  url: "/connectorslist",
  onEnter: ($transition$, $state$) => {
    // console.log('CONNECTORS ROTESSSSSSS', $transition$, $state$);
  },

  views: {
    'main@': ConnectionsComponent
  }
};


const connectrosstates = [connectorsList];
export default connectrosstates;
