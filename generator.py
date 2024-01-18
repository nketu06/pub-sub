from dataclasses import dataclass
import random

@dataclass
class personData:
    name:str
    favorite_number:int
    favorite_color:str

class generateData:
    def __init__(self) -> None:
        pass
    def getData(self):
        name="Nishat"
        favorite_number=random.randint(1,100)
        favorite_color="red"
        data=personData(name,favorite_number,favorite_color)
        return data
    
# G=generateData()
# print(vars(G.getData()))

