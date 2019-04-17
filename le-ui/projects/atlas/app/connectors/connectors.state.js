// import ConnectorList from './connectors-list.component';
// import SystemsComponent from './systems.component';
import TestComponent from './connections.component';

const connectorsList = {
  parent: 'home',
  name: "connectorslist",
  url: "/connectorslist",
  onEnter: ($transition$, $state$) => {
    console.log('CONNECTORS ROTESSSSSSS', $transition$, $state$);
  },

  views: {
    // 'main@': ConnectorList
    // 'summary@': ConnectorList,
    'main@': TestComponent
  }
};


const connectrosstates = [connectorsList];
export default connectrosstates;
