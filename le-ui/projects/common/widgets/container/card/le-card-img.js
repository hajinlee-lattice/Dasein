import React, { Component } from 'common/react-vendor';
import Aux from '../../hoc/_Aux';
import './le-card.scss';

class LeCardImg extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <div className="le-card-image-container">
                <img src={this.props.src} className={`${"le-card-image"} ${this.props.classNames ? this.props.classNames : ""}`}/>
            </div>
        );
    }
}

export default LeCardImg;