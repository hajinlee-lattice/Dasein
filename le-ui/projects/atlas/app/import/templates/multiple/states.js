import SystemCreationComponent from 'atlas/import/templates/multiple/system-creation.component';

const creationSystemState = {
  name: 'sistemcreation.systems',
  url: '/systems',
  views: {
    // Relatively target the parent-state's parent-state's 'messagecontent' ui-view
    // This could also have been written using ui-view@state addressing: 'messagecontent@mymessages'
    // Or, this could also have been written using absolute ui-view addressing: '!$default.$default.messagecontent'
    "main@": {
      component: SystemCreationComponent
    }
  }
};

export const states = [ creationSystemState];
