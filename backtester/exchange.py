from event import order_event, fill_event


class exchange:
    """
    This class handles buying and selling of securities. The portfolio sends out a buy order and the exchange handles
    the order and adds a fill order to the event queue
    """
    def __init__(self, order: order_event, queue: Queue):
        self.order = order
        self.queue = queue

    def execute_order(self):
        """
        handles executing an order. 
        Designed to experience 3 types of orders:
        1. Long
        2. Exit
        3. Short
        This order is called when 
        """
        if self.order.direction == "Long":
            # set commission with whatever you like
            fill = fill_event(amount = self.order.amount, price = self.order.price, direction = self.order.direction, commission = 0, date = self.order.date )
            fill_event.fill_cost = fill_event.amount*(fill_event.commission + fill_event.price) 
            self.queue.put(fill)
        # selling stock you own
        elif self.order.direction == "Exit":
            fill = fill_event(amount = self.order.amount, price = self.order.price, direction = self.order.direction, commission = 0, date = self.order.date )  
            fill_event.fill_cost =  fill_event.amount*fill_event.commission 
            self.queue.put(fill)
        # shorting a stock, so borrowing from the exchange then shorting
        else:
            fill = fill_event(amount = self.order.amount, price = self.order.price, direction = self.order.direction, commission = 0, date = self.order.date )  
            fill_event.fill_cost =  fill_event.amount*fill_event.commission 
            self.queue.put(fill)
