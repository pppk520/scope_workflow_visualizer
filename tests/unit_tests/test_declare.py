from unittest import TestCase
from scope_parser.declare import Declare

class TestDeclare(TestCase):
    def test_datetime_parse(self):
        s = '''
        #DECLARE RunDate DateTime = DateTime.Parse("@@RunDate@@");
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'RunDate')
        self.assertEqual(value, 'DateTime.Parse("@@RunDate@@")')

    def test_param_str(self):
        s = '''
        #DECLARE KWRawPath string = @"@@KWRawPath@@"
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'KWRawPath')
        self.assertEqual(value, '@"@@KWRawPath@@"')

    def test_int_parse(self):
        s = '''
        #DECLARE AuctionDataGetRatio int = int.Parse("@@AuctionDataGetRatio@@")
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'AuctionDataGetRatio')
        self.assertEqual(value, 'int.Parse("@@AuctionDataGetRatio@@")')

    def test_int(self):
        s = '''
        #DECLARE AuctionDataGetRatio int = 66;
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'AuctionDataGetRatio')
        self.assertEqual(value, '66')

    def test_string_format(self):
        s = '''
        #DECLARE CampaignTargetInfo string = string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'CampaignTargetInfo')
        self.assertEqual(value, 'string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate))')

    def test_big_string_format(self):
        s = '''
        #DECLARE CampaignTargetInfo String = string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'CampaignTargetInfo')
        self.assertEqual(value, 'string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate))')

    def test_multiline(self):
        s = '''
        #DECLARE BidRange string = string.Format("{0}/Result/%Y/%m/BidRange_%Y-%m-%d.ss?date={1:yyyy-MM-dd}...{2:yyyy-MM-dd}", 
            @EKWFolder, DateTime.Parse(@BTEResultStartDate), DateTime.Parse(@BTEResultEndDate));
        '''

        key, value = Declare().parse(s)
        self.assertEqual(key, 'BidRange')
        self.assertTrue('DateTime.Parse(@BTEResultStartDate)' in value)




