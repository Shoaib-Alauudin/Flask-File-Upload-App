from configparser import ConfigParser
from sqlalchemy import  create_engine

class Connection():
    def __init__(self):
        self.parser = ConfigParser()
        self.parser.read('database.config')
        self.username = self.parser.get('database_config', 'DB_USERNAME')
        self.db = self.parser.get('database_config', 'DB_Name')
        self.password = self.parser.get('database_config', 'DB_PASSWORD')
        self.address = self.parser.get('database_config', 'DB_HOST')

    def getConnection(self):
        connection = create_engine('mysql+pymysql://' + str(self.username) + ':' + str(self.password) + '@' + str(self.address) + '/' + str(self.db))
        return connection