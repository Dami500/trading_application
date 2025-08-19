



class portfolio_handler():
    """
    This class handles the portfolio of a backtesting. It keeps track of holdings,
    profits, returns, sharpe ratios and creates a visualizations
    """
    def __init__(self, initial_capital: int, symbols: List[str], queue: Queue):
        self.initial_capital = initial_capital
        self.symbols = symbols
        self.queue = queue
        self.holdings = []
        self.todays_holdings = []
        

    def 