import states from 'atlas/import/templates/multiple/states';
import states from 'atlas/import/s3files/s3files.states';
import connectrosstates from 'atlas/connectors/connectors.states';

const modulesStates = [];

function mergeStates(imported) {
    imported.forEach(state => {
        modulesStates.push(state);
    });
}

mergeStates(states);
mergeStates(connectrosstates);

export default modulesStates;