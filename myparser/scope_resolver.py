import re
import logging
from datetime import datetime
from datetime import timedelta
from dateutil import parser
from scope_parser.declare_rvalue import DeclareRvalue
from util.parse_util import ParseUtil


class ScopeResolver(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.dr = DeclareRvalue()
        self.default_datetime_obj = datetime.now() - timedelta(5)
        pass

    def check_and_eval_extend_str_cat(self, the_str):
        self.logger.debug('check_and_eval_extend_str_cat [{}]'.format(the_str))
        # for case "2018-01-01 " + 22 + ":00:00"
        if ParseUtil.is_extend_str_cat(the_str):
            parts = []
            for part in the_str.split('+'):
                part = part.strip()

                if not part.startswith('"'):
                    parts.append('"{}"'.format(part))
                    continue

                parts.append(part)

            to_eval = '+'.join(parts)
            self.logger.info('found extend_str_cat, change to_eval from [{}] to [{}]'.format(the_str, to_eval))

            # make it also look like string
            return '"{}"'.format(eval(to_eval))

        return the_str

    def resolve_datetime_parseexact(self, the_str, declare_map):
        ''' The params of DateTime.ParseExact is different from DateTime.Parse
        We need to process this separately

        :param the_str:
        :param declare_map:
        :return:
        '''
        match = re.match(r'DateTime\.ParseExact?\((.*?)\)', the_str)

        if match:
            param = match.group(1)

            items = param.split(',')
            the_datetime_str = items[0]
            # don't care the format and other calibration

            the_datetime_str = self.replace_declare_items(the_datetime_str, declare_map)
            the_datetime_str = declare_map.get(the_datetime_str, the_datetime_str)
            the_datetime_str = self.check_and_eval_extend_str_cat(the_datetime_str)

            if isinstance(the_datetime_str, str):
                # clean up before parsing
                the_datetime_str = the_datetime_str.replace('"', '')
                the_datetime_str = the_datetime_str.replace("'", '')

                self.logger.debug('try to do datetime parse [{}]'.format(the_datetime_str))
                the_datetime_str = parser.parse(the_datetime_str)

            return the_datetime_str

        return the_str

    def resolve_datetime_parse(self, the_str, declare_map):
        match = re.match(r'DateTime\.Parse?\((.*?)\)', the_str)

        if match:
            param = match.group(1)
            param = self.replace_declare_items(param, declare_map)
            param = declare_map.get(param, param)
            param = self.check_and_eval_extend_str_cat(param)

            if isinstance(param, str):
                # clean up before parsing
                param = param.replace('"', '')
                param = param.replace("'", '')

                self.logger.debug('try to do datetime parse [{}]'.format(param))
                param = parser.parse(param)

            return param

    def resolve_datetime_related(self, format_item, declare_map):
        self.logger.debug('resolve_datetime_related of [{}]'.format(format_item))

        the_data = None
        datetime_obj = None

        the_obj_name = format_item.split('.')[0]
        if the_obj_name in declare_map:
            datetime_obj = declare_map[the_obj_name]

        if 'DateTime.ParseExact' in format_item:
            datetime_obj = self.resolve_datetime_parseexact(format_item, declare_map)
            self.logger.debug('parsed datetime_obj = {}'.format(datetime_obj))
        elif 'DateTime.Parse' in format_item:
            datetime_obj = self.resolve_datetime_parse(format_item, declare_map)
            self.logger.debug('parsed datetime_obj = {}'.format(datetime_obj))

        if 'AddDays' in format_item:
            datetime_obj = self.process_add_days(datetime_obj, format_item)

        if 'ToString' in format_item:
            if datetime_obj:
                the_data = self.process_to_string(datetime_obj, format_item)

        if not the_data:
            the_data = datetime_obj

        return the_data, datetime_obj

    def resolve_basic(self, format_items, declare_map):
        ret = []

        datetime_obj = None

        for format_item in format_items:
            if format_item in declare_map:
                the_data = declare_map[format_item]
            else:
                if 'DateTime' in format_item or 'AddDays' in format_item or 'ToString' in format_item:
                    the_data, datetime_obj = self.resolve_datetime_related(format_item, declare_map)
                else:
                    # change
                    match = re.match('@"(.*)"', format_item)
                    if match:
                        the_data = match.group(1)
                    else:
                        the_data = format_item

            ret.append(the_data)

        # add quote for strings
        for i, item in enumerate(ret):
            try:
                _ = int(item)
            except ValueError:
                # it's string
                if not item.startswith('"') and item not in ['+', '-', '*', '/']:
                    ret[i] = '"{}"'.format(ret[i])

        to_eval = ''.join(ret)

        to_eval = self.check_and_eval_extend_str_cat(to_eval)

        self.logger.debug('prepare to eval [{}]'.format(to_eval))
        result = eval(to_eval)

        # final checking fot %Y %m %d
        if datetime_obj:
            result = self.to_python_datetime_format(result)
            result = datetime_obj.strftime(result)

        return result

    def is_time_format(self, the_str):
        if 'yyyy' in the_str: return True
        if 'MM' in the_str: return True
        if 'dd' in the_str: return True
        if 'HH' in the_str: return True

        return False

    def to_normalized_time_format(self, the_str):
        return the_str.replace('yyyy', '%Y')\
                      .replace('MM', '%m')\
                      .replace('dd', '%d')\
                      .replace('HH', '%H')\
                      .replace('MM', '%M')\
                      .replace('SS', '%S')

    def process_to_string(self, the_obj, func_str):
        found = re.findall('ToString\((.*?)\)', func_str)
        if found:
            if self.is_time_format(found[0]):
                return the_obj.strftime(self.to_normalized_time_format(found[0]))

            return str(the_obj)

        return the_obj

    def process_add_days(self, datetime_obj, func_str):
        found = re.findall('AddDays\((.*?)\)', func_str)
        if found:
            self.logger.debug('found AddDays [{}]'.format(found))
            return datetime_obj + timedelta(int(found[0]))

        return datetime_obj

    def resolve_func(self, func_str, declare_map={}):
        self.logger.debug('resolve_func of {}'.format(func_str))

        params = re.findall(r'\((.*?)\)', func_str)
        param = params[0].lstrip('"').rstrip('"')

        param = self.replace_declare_items(param, declare_map)

        result = func_str

        if 'DateTime.Parse' in func_str or 'AddDays' in func_str:
            result, _ = self.resolve_datetime_related(func_str, declare_map)
            self.logger.debug('resolved [{}] as datetime_obj {}'.format(func_str, result))
        elif 'ToString' in func_str:
            # check if it's DateTime.ToString('xxx')
            the_obj_name = func_str.split('.')[0]

            if the_obj_name in declare_map:
                datetime_obj = declare_map[the_obj_name]
                result = self.process_to_string(datetime_obj, func_str)
        elif func_str.startswith('int.Parse') or func_str.startswith('Int32.Parse'):
            result = int(param)
        elif func_str.startswith('Math.Abs'):
            result = abs(int(param))

        if 'ToString()' in func_str:
            # purely to string
            result = str(result)

        return result

    def resolve_ymd(self, fmt_str, datetime_obj):
        return datetime_obj.strftime(fmt_str)

    def get_format_str_declared(self, format_str, declare_map):
        if not format_str:
            return format_str

        # CASE: string.Format(@IdNamePath, @BidOptFolder, @RunDateTime, "OrderItemIdNameMap")
        if '@' + format_str in declare_map:
            format_str = declare_map['@' + format_str]

        return format_str

    def convert_datetime_obj(self, datetime_obj):
        if isinstance(datetime_obj, str):
            self.logger.warning('datetime_obj is str [{}], try to parse it.'.format(datetime_obj))
            return parser.parse(datetime_obj)

        return datetime_obj

    def has_datetime_format(self, the_str):
        if 'yyyy' in the_str or\
           'MM' in the_str or\
           'dd' in the_str or\
           '%Y' in the_str or\
           '%m' in the_str or\
           '%d' in the_str or\
           '%H' in the_str or\
           '%h' in the_str or\
           '%M' in the_str or\
           '%S' in the_str:
                return True

        return False

    def get_implicit_datetime_obj(self, format_str, format_items, declare_map):
        if self.has_datetime_format(format_str):
            datetime_obj = None

            # use the first occurrence
            for format_item in format_items:
                # check if it's declared
                format_item = declare_map.get(format_item, format_item)

                try:
                    datetime_obj = parser.parse(format_item)
                    return datetime_obj
                except Exception:
                    pass

        return None

    def to_python_datetime_format(self, format_str):
        return format_str.replace('%h', '%H')

    def resolve_str_format(self, format_str, format_items, declare_map):
        self.logger.debug('resolve_str_format of [{}], format_items = [{}]'.format(format_str, format_items))

        format_str = self.get_format_str_declared(format_str, declare_map)
        format_str = self.check_and_eval_extend_str_cat(format_str)

        placeholders = re.findall(r'{(.*?)}', format_str)

        datetime_obj = None

        # resolve placeholder values and find if any datetime obj
        for i, format_item in enumerate(format_items):
            # check if Datetime related. Use last occurrence as hidden datetime obj
            _, tmp_datetime_obj = self.resolve_datetime_related(format_item, declare_map)

            if tmp_datetime_obj:
                datetime_obj = tmp_datetime_obj

            format_items[i] = self.resolve_declare_rvalue(None, format_item, declare_map)

        self.logger.debug('format_items = ' + str(format_items))

        replace_map = {}

        for ph in placeholders:
            if ':' in ph:
                idx, fmt = ph.split(':')
                replace_to = format_items[int(idx)]

                self.logger.debug('fmt = {}, idx = {}'.format(fmt, idx))
                if self.has_datetime_format(fmt):
                    item = format_items[int(idx)]

                    if item in declare_map:
                        datetime_obj = declare_map[item]
                    else:
                        datetime_obj = item

                    datetime_obj = self.convert_datetime_obj(datetime_obj)

                    date_Y = datetime_obj.strftime('%Y')
                    date_m = datetime_obj.strftime('%m')
                    date_d = datetime_obj.strftime('%d')
                    date_h = datetime_obj.strftime('%H')

                    replace_to = fmt.replace('yyyy', date_Y)\
                                    .replace('MM', date_m)\
                                    .replace('dd', date_d) \
                                    .replace('HH', date_h)
            else:
                idx = ph
                replace_to = format_items[int(idx)]

            if not replace_to in replace_map:
                replace_map[replace_to] = []

            replace_map[replace_to].append('{' + ph + '}')

        result = format_str

        self.logger.debug('replace_map = ' + str(replace_map))

        for key in replace_map:
            if key in declare_map:
                value = declare_map[key]
            else:
                value = key

            for to_replace_str in replace_map[key]:
                result = result.replace(to_replace_str, str(value))

        if not datetime_obj:
            datetime_obj = self.get_implicit_datetime_obj(format_str, format_items, declare_map)

        # final checking fot %Y %m %d
        if datetime_obj:
            # implicit datetime in SCOPE
            if isinstance(datetime_obj, str):
                datetime_obj = parser.parse(datetime_obj)

            result = self.to_python_datetime_format(result)
            result = datetime_obj.strftime(result)

        return result

    def is_int(self, item):
        try:
            int(item)
            return True
        except Exception:
            return False

    def replace_declare_items(self, s, declare_map):
        while True:
            items = re.findall(r'(@[^ ,/\)\@]+)', s)

            if len(items) == 0:
                break

            tmp = s
            for item in items:
                if item.startswith('@@'):
                    continue # ignore external params

                if item in declare_map:
                    self.logger.debug('replace declare item [{}] to [{}]'.format(item, declare_map[item]))

                    if self.is_int(declare_map[item]):
                        tmp = tmp.replace(item, str(declare_map[item]))
                    else:
                        tmp = tmp.replace(item, '"{}"'.format(declare_map[item]))
                else:
                    self.logger.debug('item [{}] is not in declare_map'.format(item))

            if tmp == s:
                # no update, break
                break

            s = tmp

        return s

    def replace_ref_strings(self, s):
        return re.sub(r'@(".*?")', '\g<1>', s)

    def resolve_declare_rvalue(self, declare_lvalue, declare_rvalue, declare_map):
        self.logger.debug('resolve_declare_rvalue: declare_lvalue [{}], declare_rvalue [{}]'.format(declare_lvalue, declare_rvalue))

        try:
            # replace local reference of @"strings"
            declare_rvalue = self.replace_ref_strings(declare_rvalue)

            # replace those declare items inside rvalue, if any remains
            declare_rvalue = self.replace_declare_items(declare_rvalue, declare_map)

            self.logger.debug('finished replace_declare_items, start to parse.')
            ret_declare_rvalue = self.dr.parse(declare_rvalue)
        except Exception as ex:
            self.logger.warning('Exception in resolve_declare_rvalue: {}'.format(ex))
            return declare_rvalue

        format_str = ret_declare_rvalue['format_str']
        format_items = ret_declare_rvalue['format_items']
        type_ = ret_declare_rvalue['type']

        self.logger.debug('declare_rvalue type is [{}]'.format(type_))

        format_str = self.get_format_str_declared(format_str, declare_map)

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
                self.logger.warning('error resolving [{}]: {}'.format(declare_rvalue, ex))
                result = format_items[0]
        elif type_ == 'nums':
            result = format_items[0]

        self.logger.debug('[resolve_declare_rvalue] result = ' + str(result))

        # post process to change @"str" to pure "str"
        if isinstance(result, str) and '@"' in result:
            match = re.match(r'@"(.*)"', result)
            if match:
                result = match.group(1)

        # post process to remove double quotes
        if isinstance(result, str):
            result = result.lstrip('"').rstrip('"')

        return result

    def resolve_declare(self, declare_map):
        for declare_lvalue in declare_map:
            try:
                declare_rvalue = declare_map[declare_lvalue]

                # check if it references existing param, if yes directly use it
#                if declare_rvalue in declare_map:
#                    declare_map[declare_lvalue] = declare_map[declare_rvalue]
#                    continue

                resolved = self.resolve_declare_rvalue(declare_lvalue, declare_rvalue, declare_map)
                self.logger.info('update resolved [{}] to [{}]'.format(declare_lvalue, resolved))
                declare_map[declare_lvalue] = resolved
            except Exception as ex:
                self.logger.debug('Exception in resolve_declare: {}'.format(ex))
                # ignore unsupported syntax
                pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

#    result = ScopeResolver().resolve_func('@dateObj.ToString("yyyy-MM-dd")', {'dateObj': datetime.now()})
#    print(result)

    result = ScopeResolver().resolve_str_format('"/shares/bingads.algo.prod.adinsights/data/prod/pipelines/ImpressionShare/Common"+"/%Y/%m/%d/DSAMerge%Y%m%d%h.ss?date={0}&hour={1}"',
                                                ['"2018-01-01"', '22/2*2'],
                                                {})
    print(result)


#    result = ScopeResolver().resolve_declare_rvalue(None, 'string.Format("/shares/bingads.algo.prod.adinsights/data/prod/pipelines/ImpressionShare/Common"+"/%Y/%m/%d/DSAMerge%Y%m%d%h.ss?date={0}&hour={1}","2018-01-01",22/2*2)',{})
#    print(result)
