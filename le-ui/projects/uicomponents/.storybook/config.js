import { configure } from '@storybook/react';
function loadStories() {
  require('./stories/buttons.js');
  require('./stories/forms-controlls.js');
  require('./stories/containers.js');
  require('./stories/navigation.js');
  require('./stories/data-table.js');
  require('./stories/messages.js');
  require('./stories/overlay.js');
  require('./stories/panels.js');

  // You can require as many stories as you need.
}

configure(loadStories, module);