import json
from pyparsing import *
from scope_parser.common import Common


class Output(object):
    OUTPUT = Keyword("OUTPUT")
    TO = Keyword("TO")
    SSTREAM = Keyword("SSTREAM")
    WITH_STREAMEXPIRY = Keyword("WITH STREAMEXPIRY")
    PARTITIONED_BY = Keyword("PARTITIONED BY")
    USING = Keyword("USING")
    WHERE = Keyword("WHERE")
    CLUSTERED_BY = Keyword("CLUSTERED BY")
    SORTED_BY = Keyword("SORTED BY")

    ident = Common.ident
    value_str = Common.value_str
    func = Common.func

    with_streamexpiry = Group(WITH_STREAMEXPIRY + value_str)
    partitioned_by = PARTITIONED_BY + ident
    using = USING + func
    clustered_by = Group(Optional(oneOf('HASH')) + CLUSTERED_BY + ident)
    sorted_by = SORTED_BY + ident

    simple_where = Group(WHERE + ident + oneOf("== != >= <= > <") + (oneOf("true false") | value_str))

    output_sstream = OUTPUT + ((ident('ident') + TO) | TO) + Optional(SSTREAM)('sstream') + value_str('path') + \
                     Optional(clustered_by) + \
                     Optional(sorted_by) + \
                     Optional(partitioned_by)('partition') + \
                     Optional(with_streamexpiry) + \
                     Optional(simple_where) + \
                     Optional(using)

    output = output_sstream

    def parse(self, s):
        # specific output for our purpose
        ret = {
            'ident': None,
            'path': None,
            'stream_type': None,
            'attributes': set()
        }

        data = self.output.parseString(s)

#        print('-' *20)
#        print(json.dumps(data.asDict(), indent=4))
#        print('-' *20)

        if 'ident' in data:
            ret['ident'] = data['ident'][0]

        ret['path'] = data['path']

        if 'sstream' in data:
            ret['stream_type'] = 'SSTREAM'

        if 'partition' in data:
            ret['attributes'].add('PARTITION')

        return ret

if __name__ == '__main__':
    obj = Output()

    print(obj.parse('OUTPUT data TO SSTREAM "/local/tt.ss"'))
    print(obj.parse('OUTPUT TO SSTREAM "/local/tt.ss"'))
    print(obj.parse('OUTPUT AuctionMonetizationValidate TO SSTREAM @PubilsherBumpedAuction WITH STREAMEXPIRY @StreamExpiry'))
    print(obj.parse('OUTPUT AuctionMonetizationValidate TO SSTREAM @PubilsherBumpedAuction WITH STREAMEXPIRY @StreamExpiry WHERE PublisherBump==true'))

    print(obj.parse('''OUTPUT TO SSTREAM "@@TopAdvReport@@"
                       HASH CLUSTERED BY ListingFilterReason
                       WITH STREAMEXPIRY "15";'''))

    print(obj.parse('''OUTPUT IS_ORDERITEM TO @ImpressionShareReportOrderItem
                       PARTITIONED BY StripePartitionId
                       WITH STREAMEXPIRY @EXPIRY
                       USING CSVFileOutputter(@METADATA_AGG_DSV, "ImpressionShareData_OrderItem", @ASCIICsvXfmArgs_ISOutputs)'''))

    print(obj.parse('''
        OUTPUT BIOrderItem
        TO SSTREAM @BIContact
           CLUSTERED BY OrderItemId
               SORTED BY OrderItemId
           WITH STREAMEXPIRY @StreamExpiryDays;
    '''))
