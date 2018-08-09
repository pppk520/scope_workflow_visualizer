import re
import logging
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from scope_parser.declare_rvalue import DeclareRvalue


class ScopeResolver(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.dr = DeclareRvalue()
        self.default_datetime_obj = datetime.now() - timedelta(5)
        pass

    def resolve_basic(self, format_items, declare_map):
        ret = []
        for format_item in format_items:
            ret.append(declare_map.get(format_item, format_item))

        return eval(''.join(ret))

    def resolve_func(self, func_str, declare_map={}):
        params = re.findall(r'\((.*?)\)', func_str)
        param = params[0].lstrip('"').rstrip('"')

        param = declare_map.get(param, param)

        result = func_str

        if func_str.startswith('DateTime.Parse'):
            if isinstance(param, str):
                result = parser.parse(param)
            else:
                result = param
        elif func_str.startswith('int.Parse'):
            result = int(param)
        elif func_str.startswith('Math.Abs'):
            result = abs(int(param))

        if 'ToString()' in func_str:
            result = str(result)

        if 'AddDays' in func_str:
            # must be datetime already
            found = re.findall('AddDays\((.*)\)', func_str)
            if found:
                result = result + timedelta(int(found[0]))

        if isinstance(result, datetime):
            return result.strftime('%Y-%m-%d')

        return result

    def resolve_ymd(self, fmt_str, datetime_obj):
        return datetime_obj.strftime(fmt_str)

    def resolve_str_format(self, format_str, format_items, declare_map):
        placeholders = re.findall(r'{(.*?)}', format_str)

        # resolve placeholder values
        for i, format_item in enumerate(format_items):
            format_items[i] = self.resolve_declare_rvalue(None, format_item, declare_map)

        replace_map = {}
        datetime_obj = None
        for ph in placeholders:
            if ':' in ph:
                idx, fmt = ph.split(':')
                replace_to = format_items[int(idx)]

                if 'yyyy' in fmt or 'MM' in fmt or 'dd' in fmt:
                    item = format_items[int(idx)]

                    if item in declare_map:
                        datetime_obj = parser.parse(declare_map[item])

                        date_Y = datetime_obj.strftime('%Y')
                        date_m = datetime_obj.strftime('%m')
                        date_d = datetime_obj.strftime('%d')

                        replace_to = fmt.replace('yyyy', date_Y).replace('MM', date_m).replace('dd', date_d)
            else:
                idx = ph
                replace_to = format_items[int(idx)]

            if not replace_to in replace_map:
                replace_map[replace_to] = []

            replace_map[replace_to].append('{' + ph + '}')

        result = format_str

        for key in replace_map:
            if key in declare_map:
                value = declare_map[key]
            else:
                value = key

            for to_replace_str in replace_map[key]:
                result = result.replace(to_replace_str, str(value))

        # final checking
        if datetime_obj:
            result = datetime_obj.strftime(result)

        return result

    def resolve_declare_rvalue(self, declare_lvalue, declare_rvalue, declare_map):
        self.logger.debug('resolve_declare_rvalue: declare_lvalue [{}], declare_rvalue [{}]'.format(declare_lvalue, declare_rvalue))
        try:
            ret_declare_rvalue = self.dr.parse(declare_rvalue)
        except Exception as ex:
            return declare_rvalue

        format_str = ret_declare_rvalue['format_str']
        format_items = ret_declare_rvalue['format_items']
        type_ = ret_declare_rvalue['type']

        result = ""

        # update declare_map
        if type_ == 'func_chain':
            self.logger.info('found func_chain, update declare value of [{}]'.format(declare_lvalue))
            self.logger.debug('format_items = ' + str(format_items))
            result = self.resolve_func(format_items[0], declare_map)
        elif type_ == 'format_str':
            result = self.resolve_str_format(format_str, format_items, declare_map)
        elif type_ == "str_cat":
            try:
                result = self.resolve_basic(format_items, declare_map)
            except Exception as ex:
                result = format_items[0]

        return result

    def resolve_declare(self, declare_map):
        for declare_lvalue in declare_map:
            declare_rvalue = declare_map[declare_lvalue]

            print('declare_lvalue [{}], declare_rvalue [{}]'.format(declare_lvalue, declare_rvalue))
            resolved = self.resolve_declare_rvalue(declare_lvalue, declare_rvalue, declare_map)
            declare_map[declare_lvalue] = resolved


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    print(ScopeResolver().resolve_basic('66', {}))
    print(ScopeResolver().resolve_basic(['"abc"', '+', '"123"'], {'"123"': '"ABC"'}))
    print(ScopeResolver().resolve_str_format('/{0}/{1}-{2}', ['AAA', 'BBB', 'CCC'], {}))
    print(ScopeResolver().resolve_func('int.Parse("1000")'))
    print(ScopeResolver().resolve_func('DateTime.Parse("2011-01-01")'))
    print(ScopeResolver().resolve_func('Math.Abs(-1000)'))
    print(ScopeResolver().resolve_func('DateTime.Parse("11").AddDays(3)'))
    print(ScopeResolver().resolve_ymd('%Y-%m-%d', datetime.now()))
    print(ScopeResolver().resolve_str_format('/{0}/{1:yyyyMMdd}-{2}', ['AAA', 'BBB', 'CCC'], {'BBB': '2018-08-01'}))

    print(ScopeResolver().resolve_declare_rvalue('',
                                                 'string.Format("{0}/BidEstimation/Result/%Y/%m/AuctionContext_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", @BTEPath, @BTERunDate);',
                                                 {'@BTEPath': 'the_bte_path',
                                                  '@BTERunDate': '2018-08-01'}))

    print(ScopeResolver().resolve_declare_rvalue('DateDelta',
                                                 'Math.Abs(100).ToString();',
                                                 {'@DateDelta': 'Math.Abs(100).ToString();'}))


    print(ScopeResolver().resolve_declare_rvalue('',
                                                 'string.Format("{0}/BidEstimation/Result/%Y/%m/AuctionContext_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", DateTime.Parse(@RunDate), @BTERunDate);',
                                                 {'@RunDate': '2018-08-01',
                                                  '@BTERunDate': '2018-08-01'}))
