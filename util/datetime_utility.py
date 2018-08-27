from datetime import datetime, timedelta


class DatetimeUtility(object):
    @staticmethod
    def get_datetime_str(delta_days=0, fmt_str=''):
        ret = datetime.now() + timedelta(days=delta_days)

        if fmt_str:
            ret = ret.strftime(fmt_str)

        return ret


if __name__ == '__main__':
    print(DatetimeUtility.get_datetime_str(fmt_str='%Y-%m-%d'))