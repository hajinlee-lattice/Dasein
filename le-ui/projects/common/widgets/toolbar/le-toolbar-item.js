import React from 'react';
import PropTypes from 'prop-types';

const getImage = (image) => {
    if (image) {
        const classes = `${image} item-img`;
        console.log(classes);
        return <span className={classes}></span>
    } else {
        return '';
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
const LeToolbarItem = (props) => {
    return (
        <li className="le-toolbar-item" onClick={() => {clickHandler(props.callback, props.name)}}>
            <div>
                {props.children}
                {getImage(props.image)}
                {getLabel(props.label)}
            </div>
        </li>
    );
}

LeToolbarItem.propTypes = {
    name: PropTypes.string.isRequired,
    lable: PropTypes.string,
    image: PropTypes.string,
    callback: PropTypes.func
}

export default LeToolbarItem;