import ConnectionsComponent from './connections.component';
import { actions, reducer } from './connections.redux';
import { store } from 'store';

import ReactRouter from 'atlas/react/router';

const state = store.getState()['connections'];

const connectorsList = {
  parent: 'home',
  name: "connectorslist",
  url: "/connectorslist",
  onEnter: ($transition$, $state$) => {
    const isExternalIntegrationEnabled = $transition$.router.ngservices.ConnectorsService.isExternalIntegrationEnabled();
    if (isExternalIntegrationEnabled) {
      actions.fetchUserDocument();
    }

  },
  views: {
    'main@': ConnectionsComponent
  }
};


const connectrosstates = [connectorsList];
export default connectrosstates;
