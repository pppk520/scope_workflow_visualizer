import re
import logging

from util.file_utility import FileUtility

class ScriptParser(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.vars = {}

    def remove_comments(self, content):
        re_comment = re.compile(r'/\*(.*?)\*/', re.MULTILINE | re.DOTALL)

        # remove comments
        content = re.sub(re_comment, '', content)
        content = re.sub(r'[^\b;](//.*)\n', '\g<1>', content)

        return content

    def resolve_external_params(self, content, params={}):
        self.logger.debug('params = {}'.format(params))

        re_external_param = re.compile(r'@@(.*?)@@')

        def replace_matched(match):
            text = match.group()
            return params.get(match.group(1), text)

        return re_external_param.sub(replace_matched, content)

    def resolve_declare(self, part):
        regex = re.compile(r'#DECLARE[ \t]+(.*?)[ \t]+.*?=(.*)', re.DOTALL | re.MULTILINE)

        match = regex.search(part)
        if match:
            var_name = match.group(1).strip()
            var_value = match.group(2).strip()

            self.vars[var_name] = var_value

    def parse_select(self, part):
        regex = re.compile(r'[ \t]+([\w\d]+)[ \t]=.*?FROM(.*?)((WHERE(.*))|;)', re.DOTALL | re.MULTILINE)

        match = regex.search(part)
        if match:
            if match.groups()[-1] is None:
                return match.group(1).strip(), match.group(2).strip()
            else:
                return match.group(1).strip(), match.group(2).strip(), match.groups()[-1].strip()

    def parse_file(self, filepath, external_params={}):
        content = FileUtility.get_file_content(filepath)
        parts = content.split(';')

        for part in parts:
            print('-' * 20)
            print(part)


if __name__ == '__main__':
    s_declare = '''
    #DECLARE foo string = "test";
    '''

    s_select = '''
        EligibleRGuids = 
        SELECT RGUID,   
            ListingId AS OrderItemId,
            MatchTypeId,
            RelationshipId,
            DistributionChannelId,
            MediumId,
            DeviceTypeId,
            FraudQualityBand,
            NetworkId,
            DateKey,
            HourNum
        FROM (SSTREAM @EligibleRGuids);
     '''

    s = '''
    #DECLARE foo string = "test";
    
    #DECLARE abc string = "@@external@@/abc/def"; 
    #DECLARE pp string = "prefix/@@external@@/"; 
    
    '''

#    print(ScriptParser().resolve_external_params(s, {'external': 'yoyo'}))
#    print(ScriptParser().resolve_declare(s_declare))
    print(ScriptParser().resolve_select(s_select))
#    ScriptParser().parse_file('../tests/files/SOV3_StripeOutput.script')
