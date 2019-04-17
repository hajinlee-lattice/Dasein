import React from 'react';
import './toolbar.scss';

export const VERTICAL = 'vertical';
export const HORIZONTAL = 'horizontal';
export const SPACE_BETWEEN = "space-between";

const LeToolBar = (props) => {
    const classes = `${props.direction ? props.direction : ''} le-tool-bar ${props.justifycontent ? props.justifycontent : ''} ${props.className ? props.className : ''}`;
    return (
        <div className={classes}>
            {props.children}
        </div>
    );
}

export { LeToolBar };
