import re
import logging
from datetime import datetime


class ScopeResolver(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def resolve_external_params(self, lines, params={}):
        self.logger.debug('params = {}'.format(params))

        re_external_param = re.compile(r'@@(.*?)@@')

        changed_count = 0
        if len(params) != 0:
            self.logger.debug('replacing external params...')
            for idx, line in enumerate(lines):
                for occur in re.findall(re_external_param, line):
                    self.logger.debug('occur = {}'.format(occur))

                    if occur in params:
                        lines[idx] = lines[idx].replace('@@{}@@'.format(occur), '"{}"'.format(params[occur]))
                        lines[idx] = lines[idx].replace('""', '"')
                        lines[idx] = lines[idx].replace("''", "'")
                        self.logger.debug('replaced line => {}'.format(lines[idx]))

                        changed_count += 1

        return lines, changed_count

    def resolve_declares(self, lines):
        ''' Get declare dict and remove lines

        '''

        re_declare = re.compile(r'#DECLARE (.*?) [sS]tring[ ]?=[ ]?(.*?);')
        re_param_default = re.compile(r'([a-zA-Z0-9]*) [sS]tring[\b]+DEFAULT = (.*)')

        declare_d = {}
        ret_lines = []

        for line in lines:
            match = re.search(re_declare, line)

            if not match:
                ret_lines.append(line)
                continue

            key = '@{}'.format(match.group(1)).strip()
            declare_d[key] = match.group(2).strip()

            # replace with default string value
            match = re.search(re_param_default, line)
            if match:
                declare_d[key] = match.group(2).strip()

        return ret_lines, declare_d

    def resolve_variables(self, lines, params={}):
        re_param = re.compile(r'(@[a-zA-Z0-9_]*)')

        lines = resolve_external_params(lines, params=params)
        lines = self.resolve_declares(lines)
