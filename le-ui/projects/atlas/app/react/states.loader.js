import states from 'atlas/import/templates/multiple/states';
import connectrosstates from 'atlas/connectors/connectors.states';
import overviewstates from 'atlas/playbook/content/overview/overview.states';

const modulesStates = [];

function mergeStates(imported) {
    imported.forEach(state => {
        modulesStates.push(state);
    });
}

mergeStates(states);
mergeStates(connectrosstates);
mergeStates(overviewstates);

export default modulesStates;