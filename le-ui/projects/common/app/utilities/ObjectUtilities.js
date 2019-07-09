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
