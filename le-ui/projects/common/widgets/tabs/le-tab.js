import React from 'react';
import PropTypes from 'prop-types';

const getCounts = (data) => {
    if (data) {
        let counts = [];
        for (var i = 0; i < data.length; i++) {
            counts.push(<span key={i} className="count"><span>{data[i].count}</span> {data[i].label}</span>);
        }
        return <div>{counts}</div>
    } else {
        return null;
    }
}

const clickHandler = (callback, name) => {
    if(callback){
        callback(name);
    }
}

const LeTab = (props) => {
    return (
        <li className={props.classNames} onClick={() => {clickHandler(props.callback, props.name)}}>
            <div className="tab-container">
                
                <h2>{props.label}</h2>
                {getCounts(props.counts)}

            </div>
        </li>
    );
}

LeTab.propTypes = {
    name: PropTypes.string.isRequired,
    label: PropTypes.string,
    callback: PropTypes.func,
    counts: PropTypes.array
}

export default LeTab;