import order_event

class portfolio_handler():
    """
    This class handles the portfolio of a backtesting. It keeps track of holdings,
    profits, returns, sharpe ratios and creates a visualizations
    """
    def __init__(self, initial_capital: int, symbols: List[str], queue: Queue):
        self.initial_capital = initial_capital
        self.symbols = symbols
        self.queue = queue
        self.total_holdings = {}
        self.todays_holdings = {}
        self.daily_holdings = []
        self.equity_dataframe = None
        self.cash = initial_capital
        self.unclosed_shorts  = 0
        self.shorts = 0
    
    def initialize_todays_holdings(self)-> dict[str[int]]:
        """
        Before Each trading day starts, initialize all current holding to $0.
        Basically, at the begining of each day, you have no positions for that day until you see some data
        """
        return {symbol:0 for symbol in self.symbols}

    def buy_security(self, security, amount, price, date):
        """
        simulates buying a security
        """
        self.queue.put(order_event(stock = security, amount = amount, price = price, date = date, direction = "Long"))
        print(f"Buy order: {security}")
    
    def sell_security(self, security, amount, price, date):
        """
        simulates selling a security
        """
        # check if you even have any of the security to sell or exit your position
        if (self.total_holdings[security] - amount) >= 0: 
            self.queue.put(order_event(stock = security, amount = amount, price = price, date = date, direction = "Exit"))
            print(f"Sell order: {security}")
        else:
            print(f"You do not have enough {security} to sell")
    
    # all strategies involving shorting must be handled specially as 
    def short_security(self, security, amount, price, date):
        """
        simulates shorting a security
        """
        self.queue.put(order_event(stock = security, amount = amount, price = price, date = date, direction = "Short"))        