from datetime import datetime, timedelta
import re


class DatetimeUtility(object):
    @staticmethod
    def get_datetime(delta_days=0, fmt_str=''):
        ret = datetime.now() + timedelta(days=delta_days)

        if fmt_str:
            ret = ret.strftime(fmt_str)

        return ret

    @staticmethod
    def replace_ymd(ymd_str, target_datetime):
        # {yyyy-MM-dd}
        match = re.search(r'({(yyyy)?([-_/]?MM)?([-_/]?dd)?([-_/]?HH)?})', ymd_str)
        if match:
            s = match.group(1).replace('yyyy', target_datetime.strftime('%Y'))\
                              .replace('MM', target_datetime.strftime('%m'))\
                              .replace('dd', target_datetime.strftime('%d'))\
                              .replace('HH', target_datetime.strftime('%H'))[1:-1]

            return ymd_str.replace(match.group(1), s)

        return ymd_str

if __name__ == '__main__':
#    print(DatetimeUtility.get_datetime_str(fmt_str='%Y-%m-%d'))
    print(DatetimeUtility.replace_ymd('"{yyyy-MM-dd}"', datetime.now()))
    print(DatetimeUtility.replace_ymd('"{HH}"', datetime.now()))