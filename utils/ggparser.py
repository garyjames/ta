from IEXTools import Parser, messages

class TradeParser(Parser):
    def __init__(self, filepath):
        self.filter_tradereport = [messages.TradeReport]
        super().__init__(filepath)

    def __next__(self):
        return self.get_next_message(self.filter_tradereport)
