class DataHandler:
    """
    This is an Abstract base Class of the data handler of this trading system.
    Its purpose is to load prices into a pandas Dataframe then generate
    these prices one by one sequentially till the end of the trading period.
    """
    def __init__(self, symbols: list, queue: Queue):
        self.symbols = symbols
        self.latest_symbol_data = {}
        self.queue = queue

    def load(self)-> None:
        """
        This method loads the OHLCV data from any database or data source of
        the users choosing
        :return:
        """
        raise NotImplementedError

    def generate(self) ->dict:
        raise NotImplementedError

    def add_market_event(self) -> None:
        raise NotImplementedError


