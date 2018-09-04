import React from 'react';
import './toolbar.scss';
import '../layout/layout.scss';


export const VERTICAL = 'vertical';
export const HORIZONTAL = 'horizontal';

const getDirection = (props) => {
    if (props.direction === 'vertical') {
        return ['le-toolbar', 'vertical'];

    } else {
        return ['le-toolbar', 'horizontal'];
    }
}


const LeToolbar = (props) => {


    return (
        <div className={getDirection(props).join(' ')}>
            <ul className={getDirection(props).join(' ')}>
                {props.children}
            </ul>
        </div>
    );
}

const Toolbar = (props) => {


    return (
        <div className="le-flex-h-panel tool-bar">
            {props.children}
        </div>
    );
}

export { Toolbar };
export default LeToolbar;
