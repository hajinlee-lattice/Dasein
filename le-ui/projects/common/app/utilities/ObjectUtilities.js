// Containes the utilities to work with js objects

 export const isEmpty = (obj) => {
    for(var key in obj) {
        if(obj.hasOwnProperty(key))
            return false;
    }
    return true;
}

export const deepCopy = (obj) => {
    if(obj && Object.keys(obj).length) {
        return JSON.parse(JSON.stringify(obj));
    }
    return {};
}

export const isEquivalent = (object1, object2) => {
    var object1Props = Object.getOwnPropertyNames(object1);
    var object2Props = Object.getOwnPropertyNames(object2);

    // If number of properties is different
    if (object1Props.length != object2Props.length) {
        return false;
    }

    for (var i = 0; i < object1Props.length; i++) {
        var propName = object1Props[i];

        // If values of same property are not equal
        if (object1[propName] !== object2[propName]) {
            return false;
        }
    }
    return true;
}
