import React, { Component } from 'common/react-vendor';
import Aux from '../../hoc/_Aux';
import './le-card.scss';

class LeCardImg extends Component {
    constructor(props) {
        super(props);
    }
    render() {

        return (
            <Aux>
                <img src={this.props.src} className={`${this.props.classNames ? this.props.classNames : ""}`}/>
            </Aux>
        );
    }
}

export default LeCardImg;