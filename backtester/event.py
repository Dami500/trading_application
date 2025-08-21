
class event:
    """
    This class represents the base class for which 
    all the events in this backtester will be based on
    """
    def __init__(self):
        self.type = None


class market_event(event):
    """
    This class represents the event that some new data has been introduced into the market.
    Typically new prices or maybe a news feed
    """
    def __init__(self, info):
        """
        Initializes the type and info (bar or chunk of information/OHLCV + date)
        """
        self.type = "Market"
        self.info = info

class signal_event(event):
    """
    This class represents the signal calculated by the trading strategy it can be a LONG or SHORT signal
    """
    def __init__(self, direction, symbols, strategy_id):
        self.type = "Signal"
        self.direction = direction
        self.symbols = symbols
        self.strategy_id = strategy_id

class order_event(event):
    """
    This class represents an order that is to be executed and accounted for by the portfolio handler
    """
    def __init__(self, stock, amount, price, date, direction):
        self.stock = stock
        self.type = "Order"
        self.direction = direction
        self.amount = amount
        self.price = price
        self.date = date

class fill_event(event):
    """
    This class represents a filled event. Thus after an order has been made, the exchange respondes with a filled event
    """
    def __init__(self, amount, price, direction, commission, date):
        self.type = "Fill"
        self.amount = amount
        self.commission = commission
        self.price = price
        self.fill_cost = 0
        self.direction = direction
        self.date = date

