from commontypes import FxCorrelatedPair

class AcmeBroker:
    def __init__(self, feeds_folder):
        self.feeds_folder = feeds_folder
        self.feeds = []
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(1).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(1), "M15",
                      "EURUSDGBP-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(2).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(2), "M15",
                      "EURUSDAUD-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(3).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(3), "M15",
                      "EURUSDNZD-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(4).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(4), "M15",
                      "CHFUSDJPY-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(5).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(5), "M15",
                      "AUDUSDNZD-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(6).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(6), "M15",
                      "GBPUSDJPY-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(7).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(7), "M15",
                      "EURUSDCHF-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(8).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(8), "M15",
                      "GBPUSDCHF-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(9).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(9), "M15",
                      "AUDUSDJPY-2012-09-2017-09-M15.ohlcfeed"
                      ))
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(10).value.name,
                      FxCorrelatedPair.get_correlated_pair_by_id(10), "M15",
                      "CADUSDAUD-2012-09-2017-09-M15.ohlcfeed"
                      ))
        #M5
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(8).value.name,
                           FxCorrelatedPair.get_correlated_pair_by_id(8), "M5",
                           "GBPUSDCHF-2012-09-2017-09-M5.ohlcfeed"
                           ))
        #M1
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(8).value.name,
                           FxCorrelatedPair.get_correlated_pair_by_id(8), "M1",
                           "GBPUSDCHF-2012-09-2017-09-M1.ohlcfeed"
                           ))

        # M5
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(1).value.name,
                           FxCorrelatedPair.get_correlated_pair_by_id(1), "M5",
                           "EURUSDGBP-2012-09-2017-09-M5.ohlcfeed"
                           ))

        # M1
        self.feeds.append((FxCorrelatedPair.get_correlated_pair_by_id(1).value.name,
                           FxCorrelatedPair.get_correlated_pair_by_id(1), "M1",
                           "EURUSDGBP-2012-09-2017-09-M1.ohlcfeed"
                           ))

    def get_feed(self, correlated_pair_name, timeframe):
        for feed in self.feeds:
            if correlated_pair_name.lower() == feed[0].lower() and feed[2].lower() == timeframe.lower():
                return feed
        return None
