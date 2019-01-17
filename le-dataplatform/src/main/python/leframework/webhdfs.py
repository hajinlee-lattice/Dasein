import httplib
import json
import logging
import os
import re
import urlparse


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
logger = logging.getLogger(name='webhdfs')

WEBHDFS_CONTEXT_ROOT = "/webhdfs/v1"


def dns_to_ip(addr):
	pattern = "ip-(?P<p1>\\d+)-(?P<p2>\\d+)-(?P<p3>\\d+)-(?P<p4>\\d+)"
	match = re.match(pattern, addr)
	if match:
		return "%s.%s.%s.%s" % (match.group("p1"), match.group("p2"), match.group("p3"), match.group("p4"))
	else:
		return addr


def replace_dns_name(message):
	pattern1 = "ip-\\d+-\\d+-\\d+-\\d+\\.lattice\\.local"
	pattern2 = "ip-\\d+-\\d+-\\d+-\\d+\\.ec2\\.internal"
	pattern3 = "ip-\\d+-\\d+-\\d+-\\d+"
	for addr in re.findall(pattern1, message):
		ip = dns_to_ip(addr)
		message = message.replace(addr, ip)
	for addr in re.findall(pattern2, message):
		ip = dns_to_ip(addr)
		message = message.replace(addr, ip)
	for addr in re.findall(pattern3, message):
		ip = dns_to_ip(addr)
		message = message.replace(addr, ip)
	return message


class WebHDFS(object):
    """ Class for accessing HDFS via WebHDFS

        To enable WebHDFS in your Hadoop Installation add the following configuration
        to your hdfs_site.xml (requires Hadoop >0.20.205.0):

        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>

        see: https://issues.apache.org/jira/secure/attachment/12500090/WebHdfsAPI20111020.pdf
    """

    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host = replace_dns_name(namenode_host)
        self.namenode_port = namenode_port
        self.username = hdfs_username


    def mkdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))

        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=MKDIRS&user.name=' + self.username
        url_path = replace_dns_name(url_path)
        logger.debug("Create directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        httpClient.close()


    def rmdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))

        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=DELETE&recursive=true&user.name=' + self.username
        url_path = replace_dns_name(url_path)
        logger.debug("Delete directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('DELETE', url_path, headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        httpClient.close()


    def copyFromLocal(self, source_path, target_path, replication=3):
        if os.path.isabs(target_path) == False:
            raise Exception("Only absolute paths supported: %s" % (target_path))

        url_path = WEBHDFS_CONTEXT_ROOT + target_path + '?op=CREATE&overwrite=true&user.name=' + self.username
        url_path = replace_dns_name(url_path)

        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        msg = response.msg
        redirect_location = msg["location"]
        logger.debug("HTTP Location: %s" % (redirect_location))
        result = urlparse.urlparse(redirect_location)
        redirect_host = result.netloc[:result.netloc.index(":")]
        redirect_port = result.netloc[(result.netloc.index(":") + 1):]
        # Bug in WebHDFS 0.20.205 => requires param otherwise a NullPointerException is thrown
        redirect_path = result.path + "?" + result.query + "&replication=" + str(replication)

        redirect_host = replace_dns_name(redirect_host)
        redirect_path = replace_dns_name(redirect_path)
        logger.debug("Send redirect to: host: %s, port: %s, path: %s " % (redirect_host, redirect_port, redirect_path))
        fileUploadClient = httplib.HTTPConnection(redirect_host, redirect_port, timeout=600)
        # This requires currently Python 2.6 or higher
        fileUploadClient.request('PUT', redirect_path, open(source_path, "r").read(), headers={"Content-Type":"application/octet-stream"})
        response = fileUploadClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        httpClient.close()
        fileUploadClient.close()
        return response.status


    def copyToLocal(self, source_path, target_path):
        if os.path.isabs(source_path) == False:
            raise Exception("Only absolute paths supported: %s" % (source_path))
        url_path = WEBHDFS_CONTEXT_ROOT + source_path + '?op=OPEN&overwrite=true&user.name=' + self.username
        url_path = replace_dns_name(url_path)
        logger.debug("GET URL: %s" % url_path)
        logger.debug("Namenode: %s" % self.namenode_host)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        # if file is empty GET returns a response with length == NONE and
        # no msg["location"]
        if response.status == 200 and self.__isNameNodeHA():
            target_file = open(target_path, "w")
            target_file.write(response.read())
            target_file.close()
        elif response.length != None:
            msg = response.msg
            logger.debug("HTTP Response: %s" % (response.msg))
            redirect_location = msg["location"]
            logger.debug("HTTP Location: %s" % (redirect_location))
            result = urlparse.urlparse(redirect_location)
            redirect_host = result.netloc[:result.netloc.index(":")]
            redirect_port = result.netloc[(result.netloc.index(":") + 1):]

            redirect_path = result.path + "?" + result.query

            redirect_host = replace_dns_name(redirect_host)
            redirect_path = replace_dns_name(redirect_path)
            logger.debug("Send redirect to: host: %s, port: %s, path: %s " % (redirect_host, redirect_port, redirect_path))
            fileDownloadClient = httplib.HTTPConnection(redirect_host, redirect_port, timeout=600)
            fileDownloadClient.request('GET', redirect_path, headers={"Content-Type":"application/octet-stream"})
            response = fileDownloadClient.getresponse()
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))

            # Write data to file
            target_file = open(target_path, "w")
            target_file.write(response.read())
            target_file.close()
            fileDownloadClient.close()
        else:
            target_file = open(target_path, "w")
            target_file.close()

        httpClient.close()
        return response.status


    def listdir(self, path):
        if os.path.isabs(path) == False:
            raise Exception("Only absolute paths supported: %s" % (path))

        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=LISTSTATUS&user.name=' + self.username
        url_path = replace_dns_name(url_path)
        logger.debug("List directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        if response.status == 404: return []
        data_dict = json.loads(response.read())
        logger.debug("Data: " + str(data_dict))
        files = []
        for i in data_dict["FileStatuses"]["FileStatus"]:
            logger.debug(i["type"] + ": " + i["pathSuffix"])
            files.append(i["pathSuffix"])
        httpClient.close()
        return files

    def __getNameNodeHTTPClient(self):
        httpClient = httplib.HTTPConnection(self.namenode_host,
                                            self.namenode_port,
                                            timeout=600)
        return httpClient

    def __isNameNodeHA(self):
        return self.namenode_port == 14000
