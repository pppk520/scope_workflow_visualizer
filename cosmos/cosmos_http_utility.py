import pycurl
import os
import re
import json
import logging
import configparser
from io import BytesIO
from bs4 import BeautifulSoup


class CosmosHttpUtility(object):
    logger = logging.getLogger(__name__)

    def __init__(self, auth_config_path):
        config = configparser.ConfigParser()
        config.optionxform = str  # reserve case
        config.read(auth_config_path)

        self.host = config['Cosmos']['host']
        self.username = config['Cosmos']['username']
        self.password = config['Cosmos']['password']

        self.logger.debug('host = [{}]'.format(self.host))
        self.logger.debug('username = [{}]'.format(self.username))

    def get_auth_curl(self):
        name = self.username
        pwd = self.password

        curl = pycurl.Curl()
        curl.setopt(pycurl.URL, self.host)
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)

        curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_NTLM)
        curl.setopt(pycurl.USERPWD, "{}:{}".format(name, pwd))
        # make it silent
        curl.setopt(pycurl.WRITEFUNCTION, lambda x: None)

        curl.perform()

        return curl

    def get(self, the_url, to_folder=None, append_ext=''):
        self.logger.debug('get the_url [{}]'.format(the_url))
        buffer = BytesIO()

        curl = self.get_auth_curl()
        curl.setopt(pycurl.URL, the_url)
        curl.setopt(pycurl.WRITEDATA, buffer)
        curl.perform()
        curl.close()

        content = buffer.getvalue().decode('iso-8859-1')

        if to_folder:
            filepath = '/'.join((to_folder, os.path.basename(the_url) + append_ext))
            with open(filepath, "w") as fw:
                fw.write(content)

        return content

    def get_ss_list(self, relpath):
        the_url = "{}/{}".format(self.host, relpath)

        page = self.get(the_url)
        return self.get_ss_list_from_page(page)

    def download_ss_csv(self, relpath, to_folder):
        the_url = "{}/File/DownloadStructuredStream/{}".format(self.host, relpath)
        self.get(the_url, to_folder=to_folder, append_ext='.csv')

    def get_folders(self, relpath):
        the_url = "{}/{}".format(self.host, relpath)

        self.logger.debug('get_folders of [{}]'.format(the_url))
        page = self.get(the_url)
        return self.get_folders_from_page(page)

    def post_for_ajax_data(self, relpath, num_rows=100):
        assert(num_rows <= 10000)

        ajax_preview_prefix = r'https://cosmos08.osdinfra.net/File/StructuredStreamAjaxPreview'

        the_url = "{}/{}".format(ajax_preview_prefix, relpath)
        the_url += '&numberOfRows={}'.format(num_rows)

        buffer = BytesIO()

        data = {'sort': '',
                'group': '',
                'filter': ''}

        curl = self.get_auth_curl()
        curl.setopt(pycurl.URL, the_url)
        curl.setopt(pycurl.HTTPHEADER, ['Accept:application/json'])
        curl.setopt(pycurl.WRITEDATA, buffer)
        curl.setopt(pycurl.POSTFIELDS, json.dumps(data))
        curl.setopt(pycurl.POST, 1)
        curl.perform()

        curl.close()

        self.logger.debug(buffer.getvalue())

        return json.loads(buffer.getvalue().decode('iso-8859-1'))['Data']

    def get_ss_list_from_page(self, html_doc):
        ss_list = []

        soup = BeautifulSoup(html_doc, 'html.parser')

        for cell in soup.select("#Grid > table"):
            for item in cell.tbody:
                ss = self.parse_ss(str(item))
                if not ss:
                    continue

                ss_list.append(ss)

        return ss_list

    def get_folders_from_page(self, html_doc):
        ll = []

        soup = BeautifulSoup(html_doc, 'html.parser')

        for item in soup.find_all('a', class_='unmountedDirLink'):
            ll.append(item.string)

        return ll

    def parse_ss(self, content):
        match = re.search(r"href=\"(.*\.ss\?property=info)\">", content)
        if match:
            return match.group(1)

    def get_ss_top_n(self, the_url):
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    hu = CosmosHttpUtility("d:/workspace/dummydummy.ini")

    print(hu.get("https://nonexist.com"))

