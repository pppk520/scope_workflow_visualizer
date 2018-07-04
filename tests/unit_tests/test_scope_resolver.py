import unittest

from myparser.scope_resolver import ScopeResolver

class TestScopeResolver(unittest.TestCase):
    test_scope_script = '''
    #DECLARE FeatureAdoptionOptView string = "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/Views/FeatureAdoptionOptFromPkvTable.view";
    #DECLARE BTEPath string = @"/shares/bingads.algo.prod.adinsights/data/shared_data/AdvertiserEngagement/Metallica/prod/BidEstimation/";
    #DECLARE AdGroupPerf7D string = string.Format("{0}/SubjectArea/%Y/%m/AdGroup.Perf.All.%Y-%m-%d.ss?date={1:yyyy-MM-dd}...{2:yyyy-MM-dd}", @BTEPath, @RunDate.AddDays(-6), @RunDate);
    #DECLARE CampaignPerf7D string = string.Format("{0}/SubjectArea/%Y/%m/Campaign.Perf.All.%Y-%m-%d.ss?date={1:yyyy-MM-dd}...{2:yyyy-MM-dd}", @BTEPath, @RunDate.AddDays(-6), @RunDate);

    #DECLARE DllPath string = "@@DllPath@@"; 

    REFERENCE @MSBICommonDllPath;
    REFERENCE @MSBIScopePipelineDllPath;
    USING Microsoft.BI.ScopePipelines;
    
    #DECLARE MonetizationModulePath    string = string.Format("{0}/Modules/Monetization.module", @CdlFolder);
    #DECLARE NafModulePath             string = string.Format("{0}/Modules/NAF.module", @CdlFolder);
    #DECLARE C2CModulePath		   string = string.Format("{0}/Modules/C2C.module", @CdlFolder);
    MODULE   @MonetizationModulePath   AS MonetizationModules;
    MODULE   @NafModulePath            AS NAFModules;
    MODULE   @C2CModulePath            AS C2CModules;
    '''

    def setUp(self):
        self.sr = ScopeResolver()
        self.lines = self.test_scope_script.splitlines()

    def test_resolve_external_params(self):
        lines = self.lines

        lines, change_count = self.sr.resolve_external_params(lines)
        self.assertTrue(change_count == 0)

        lines, change_count = self.sr.resolve_external_params(lines, params={'DllPath': 'test_path'})
        self.assertTrue(change_count == 1)


    def test_get_declares(self):
        lines = self.lines

        ret_lines, d = self.sr.resolve_declares(lines)

        self.assertTrue(len(ret_lines) + len(d) == len(lines))

if __name__ == '__main__':
    unittest.main()