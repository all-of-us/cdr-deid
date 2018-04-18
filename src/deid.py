"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
    Policies are the application of operations on a record :
        - The operations can be applied on data-types
        - The operations can be applied on tables (suppression of fields)
    
    @TODO:
    
    
"""

import json
from google.cloud import bigquery as bq
from google.cloud import storage

class BQHandler:
	"""
		This is a Big Query handler that is intended to serve as an interface to bq
	"""
	def __init__(self,path):
            """
                Initializing the big query handler with a service account 
                @TODO: 
                    Authentication policy is not clear, talk to an architect about this .
                @param path path of the service account file
            """
            self.path = path
            self.client = bq.Client.from_service_account_json(path) 
            self.init()
            
        def init (self) :
            """
                This function loads all of the dataset references so we can work with them upon Initialization
            """
            self.datasets = list(self.client.list_datasets())
            pass
        def get_dataset(self,name):
            """
                This function returns a dataset given a name (perhaps the list of tables should be returned)
                @param nam name of a table
            """
            for row in self.datasets :
                if name == row.dataset_id :
                    return row
            return None
            
        def get_tables (self,name):
            """
               This function will return a table given a dataset and 
               @param dbame dataset name
               @param tname
            """
            dataset = self.get_dataset(name)
            return list(self.client.list_tables(dataset.reference))
        def meta(self,dataset_name,table_name):
            """
                This function will return meta data about a table provided dataset name and table name
                @param dataset_name     name of the dataset
                @param table_name       name of the table
            """
            table = [table for table in self.get_tables(dataset_name) if table.table_id == table_name ]
            if table :
                table = table[0]
                table = self.client.get_table(table.reference)
                print table.schema[0]
                print table.schema[0].name,' ** ',table.schema[0].field_type, ' ** ',table.schema[0].mode
            
		
class Deid:
	"""
		This class is intended to apply a given policy from one table to another
		The application of a policy is a safe-harbor like type of policy i.e suprression of fields.
	"""
	def __init__(self):
		pass
            
handler = BQHandler('config/account/account.json')
handler.init()
handler.meta('raw','concept')