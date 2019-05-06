import states from 'atlas/import/templates/multiple/states';
import s3filestates from 'atlas/import/s3files/s3files.states';
import connectrosstates from 'atlas/connectors/connectors.states';
import overviewstates from 'atlas/playbook/content/overview/overview.states';
import statessingle from 'atlas/import/templates/states';
// projects/atlas/app/import/templates/states.js
const modulesStates = [];

function mergeStates(imported) {
    imported.forEach(state => {
        modulesStates.push(state);
    });
}

mergeStates(states);
mergeStates(connectrosstates);
mergeStates(overviewstates);
mergeStates(s3filestates);
mergeStates(statessingle);

export default modulesStates;