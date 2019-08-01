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
