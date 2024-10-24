from IEXTools import Parser, messages

class MyParser(Parser):
    def __init__(self, filepath):
        self.filter_tradereport = [messages.TradeReport]
        super().__init__(filepath)

    def __next__(self):
        return self.get_next_message(self.filter_tradereport)


#parser = MyParser(filepath, message_filter)

#def func(x):
#    for i in x:
#        yield [ i.timestamp, i.symbol, i.size, i.price, i.trade_id ]
