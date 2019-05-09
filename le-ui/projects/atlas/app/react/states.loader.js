import states from 'atlas/import/templates/multiple/states';
import s3filestates from 'atlas/import/s3files/s3files.states';
import connectrosstates from 'atlas/connectors/connectors.states';
import overviewStates  from 'atlas/playbook/content/overview/overview.states';

const modulesStates = [];

function mergeStates(imported) {
    imported.forEach(state => {
        modulesStates.push(state);
    });
}

mergeStates(states);
mergeStates(connectrosstates);
mergeStates(overviewStates);

export default modulesStates;