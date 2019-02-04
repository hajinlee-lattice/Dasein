import React from 'react';
import './toolbar.scss';

export const VERTICAL = 'vertical';
export const HORIZONTAL = 'horizontal';


export const LeToolBar = (props) => {
    const classes = `${props.direction ? props.direction: ''} le-tool-bar`;
    return(
        <ul className={classes}>
            {props.children}
        </ul>
    );
}