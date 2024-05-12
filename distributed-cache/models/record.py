from datetime import datetime
from models.access_details import AccessDetails

class Record:
    def __init__(self, key, value, insertion_time):
        self.key = key
        self.value = value
        self.insertion_time = insertion_time
        self.access_details = AccessDetails(insertion_time)

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value

    def get_insertion_time(self):
        return self.insertion_time

    def get_access_details(self):
        return self.access_details

    def set_access_details(self, access_details):
        self.access_details = access_details

    def __str__(self):
        return f"Record(key={self.key}, value={self.value}, insertionTime={self.insertion_time}, accessDetails={self.access_details})"
