import logging
import tempfile
import json
import os
from cosmos.cosmos_http_utility import CosmosHttpUtility
from bs4 import BeautifulSoup


class SstreamUtility(object):
    logger = logging.getLogger(__name__)

    GET_FAIL_MAX = 3

    def __init__(self, auth_config_path, cache_filename='sstream_utility_cache.json'):
        self.hu = CosmosHttpUtility(auth_config_path)

        self.cache_folder = tempfile.gettempdir()
        self.cache_filepath = '' # init as nothing
        self.cache = {}

        if cache_filename:
            self.cache_filepath = os.path.join(self.cache_folder, cache_filename)
            self.load_cache()

        self.fail_cache = {}


    def refresh_cache(self):
        self.save_cache()
        self.load_cache()

    def load_cache(self):
        if not os.path.exists(self.cache_filepath):
            return {}

        with open(self.cache_filepath) as fp:
            self.cache = json.load(fp)

    def save_cache(self):
        ''' Call from external process. No race-condition protection
        '''
        with open(self.cache_filepath, 'w') as fw:
            json.dump(self.cache, fw, indent=4)

        self.logger.info('saved cache info to [{}]'.format(self.cache_filepath))

    def update_cache(self, the_url, key, value):
        if the_url not in self.cache:
            self.cache[the_url] = {}

        self.cache[the_url][key] = value

    def get_normalize_url(self, the_url):
        if not the_url.endswith('?property=info'):
            the_url += "?property=info"

        self.logger.debug('the_url = {}'.format(the_url))
        return the_url

    def get_schema(self, data_url):
        data_url = self.get_normalize_url(data_url)

        the_html = self.hu.get(data_url)
        soup = BeautifulSoup(the_html, 'html.parser')

        ret = {}
        for row in soup.find_all("div", class_="div-table-row"):
            items = row.find_all("div", class_="div-table-col")
            ret[items[0].string] = items[1].string

        return ret

    def sizeof_fmt(self, num, suffix=''):
        ''' https://stackoverflow.com/a/1094933/1004325
        '''
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 1024.0:
                return "%3.1f %s%s" % (num, unit, suffix)
            num /= 1024.0

        return "%.1f %s%s" % (num, 'Yi', suffix)

    def ss_info_size_pretty(self, ss_info_size_raw):
        # 169,299 Bytes
        byte_num = ss_info_size_raw.split(' ')[0].replace(',', '')

        return self.sizeof_fmt(int(byte_num))

    def get_stream_size(self, data_url):
        data_url = self.get_normalize_url(data_url)
        key = 'size'

        if data_url in self.cache and key in self.cache[data_url]:
            return self.cache[data_url][key]

        the_html = self.hu.get(data_url)

        try:
            soup = BeautifulSoup(the_html, 'html.parser')

            div = soup.find(id="details_fileinfo")
            for row in div.find_all('tr'):
                if 'File Size' in row.text:
                    filesize_pretty = self.ss_info_size_pretty(row.find('td').text)
                    self.update_cache(data_url, key, filesize_pretty)

                    return filesize_pretty
        except Exception as ex:
            self.logger.warning(ex)

        if data_url not in self.fail_cache:
            self.fail_cache[data_url] = 0

        self.fail_cache[data_url] += 1

        if self.fail_cache[data_url] > self.GET_FAIL_MAX:
            self.logger.warning('failed getting size from url [{}]'.format(data_url))
            self.update_cache(data_url, key, 'NA')

        return ''


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    ss_path = "https://nonexist"

#    print(SstreamUtility("d:/workspace/dummydummy.ini").get_schema(ss_path))
    print(SstreamUtility("d:/workspace/dummydummy.ini").get_stream_size(ss_path))
