import os 
import pickle

class State:
    def __init__(self, data_path: str): 
        current_path = os.path.dirname(__file__) + "../"
        self.data_path = os.path.join(current_path, data_path)

    def save_state(self):  
        f = open(self.data_path, 'wb+')        
        pickle.dump(self, f)

    @staticmethod
    def get_state_from_file(data_path: str): 
        if os.path.exists(data_path):
            f = open(data_path, 'rb')
            state = pickle.load(f)
            f.close() 
            return state
        return None
        