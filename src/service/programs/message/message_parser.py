class MessageParser:

    def __init__(self):
        pass

    @staticmethod
    def encode(messages): 
        for i in range(len(messages)):
            messages[i] = str(messages[i]).encode('utf-8') 
        return messages

    @staticmethod
    def decode(messages): 
        print(messages)
        for i in range(len(messages)):
            messages[i] = messages[i].decode('utf-8')
        return messages
