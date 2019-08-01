/**
 * This class is including the utils to manipulate the urls
 * It is NOT a singleton
 */
class UrlUtils {
	constructor() {}
	encodeUrl(url) {
		if (url) {
			return encodeURIComponent(url);
		} else {
			return "";
		}
	}
}

const instance = new UrlUtils();

export default instance;
