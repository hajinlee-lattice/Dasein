import React, { Component } from '../../../react-vendor';
import Aux from '../../hoc/_Aux';
import './le-tile.scss';

class LeTile extends Component {
    constructor(props){
        super(props);
    }
    render() {

        return (
            <Aux>
                <div className="le-flex-v-panel le-tile">
                    {this.props.children}
                </div>
            </Aux>

        );
    }
}

export default LeTile;