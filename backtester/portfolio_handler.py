from event import order_event
import queue

class portfolio_handler():
    """
    This class handles the portfolio of a backtesting. It keeps track of holdings,
    profits, returns, sharpe ratios and creates a visualizations
    """
    def __init__(self, initial_capital: int, symbols: list[str], queue: queue):
        self.initial_capital = initial_capital
        self.symbols = symbols
        self.queue = queue 
        self.todays_holdings = self.initialize_todays_holdings()
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
    
    # all strategies involving shorting must be handled specially including a day when the user intends to buy back
    # what they have shorted
 
    def short_security(self, security, amount, price, date):
        """
        simulates shorting a security
        """
        self.queue.put(order_event(stock = security, amount = amount, price = price, date = date, direction = "Short")) 

    def update_holdings_and_capital_from_fill(self):
        """
        updates todays holdings and appends whatever has been bought or sold for the day and adds
        it to the total holdings data structure
        """ 
        # next item in the queue should always be a fill event. So after every buy there needs to be a fill before
        # countinuing the backtesting process
        
        fill = self.queue.get()

        if fill.type == "Fill":
            amount = fill.amount
            date = fill.date
            price = fill.price
            
            if fill.direction == "Long":
                self.todays_holdings[fill.security] += amount*price
                self.cash -= amount*price
            
            elif fill.direction == "Short":
                self.todays_holdings[fill.security] -= amount*price
                self.cash += amount*price
            
            else:
                self.cash += amount*price
            
            self.todays_holdings["Date"] = date
        else:
            print("Error! the next item in the queue after a long buy/sale or short should always" \
            "be fill event")

    # TO-DO
    def create_equity_curve_dataframe(self):
        """
        This creates the equity curve dataframe from the daily holdings.
        The dataframe would involve the following as columns

        1. all the keys from the daily holdings dictionary
        2. daily returns
        """ 
        pass

    def output_summary_statistics(self):
        """
        Outputs the following at the end of the backtest:

        1. Returns plot
        2. Maximum drawdown
        3. sharpe ratio
        4. signals generated
        5. total number of orders and fills
        6. drawdown graph
        """
        pass

