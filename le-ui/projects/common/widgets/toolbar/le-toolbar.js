import React from 'react';
import './toolbar.scss';

export const VERTICAL = 'vertical';
export const HORIZONTAL = 'horizontal';


const LeToolBar = (props) => {
    const classes = `${props.direction ? props.direction: ''} le-tool-bar`;
    return(
        <div className={classes}>
            {props.children}
        </div>
    );
}

export { LeToolBar };
