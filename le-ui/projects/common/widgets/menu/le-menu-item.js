import React from 'react';
import PropTypes from 'prop-types';

const getImage = (image) => {
    if (image) {
        const classes = `${image} item-img`;
        return <span className={classes}></span>
    } else {
        return <span className="place-holder"></span>;
    }
}

const getLabel = (label) => {
    if (label) {
        return <span className="label">{label}</span>
    } else {
        return null;
    }
}



const clickHandler = (callback, name) => {
    if(callback){
        callback(name);
    }
}

/**
 * 
 * @param {*} props 
 * name
 * label
 * image
 * callback
 * Refer to propTypes for the types
 */
const LeMenuItem = (props) => {
    return (
        <li className="le-menu-item" onClick={() => {clickHandler(props.callback, props.name)}}>
            <div>
                
                {getImage(props.image)}
                {getLabel(props.label)}
                
            </div>
            {props.children}
        </li>
    );
}

LeMenuItem.propTypes = {
    name: PropTypes.string.isRequired,
    lable: PropTypes.string,
    image: PropTypes.string,
    callback: PropTypes.func
}

export default LeMenuItem;