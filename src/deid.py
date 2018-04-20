"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
    Policies are the application of operations on a record :
        - The operations can be applied on data-types
        - The operations can be applied on tables (suppression of fields)
    
    @TODO:
    
    
"""
from __future__ import division
import json
from google.cloud import bigquery as bq

class Policy :
    """
        This function will apply Policies given the fields found on a given table
        The policy hierarchy will be applied as an iterator design pattern.
    """
    @staticmethod
    def instance(meta):
        """
            This function will return one or severeal instances of a policies associated with the meta data
        """
        return None
    
    def __init__(self,**args):
        """
            This function loads basic specifications for a policy
        """
        self.fields = []
        self.policies = {}
        if 'client' in args :
            self.client = args['client']
        elif 'path' in args :
            self.client = bq.Client.from_service_account_json(args['path'])

    def can_do(self,id,meta):
        """
            This function will determine if a table has fields that are candidate for suppression
            
            @param id       table identifier
            @param meta     list of SchemaField objects
        """
        if id not in self.cache :
            if id in self.policies and id not in self.cache:
                info = self.policies[id]
                self.cache[id] = sum([ int(field.name in info) for field in meta ]) > 0
            else:
                self.cache[id] = False
        return self.cache[id]
    
    def do(self,**args) :
        return None
    
class Suppress(Policy):
    """
        This class implements supression rules on a table (specified by meta data)
        
    """
    def __init__(self,**args):
        """
            @param policy     contains table with list of fields to be suppressed
        """
        Policy.__init__(self,**args)
        self.policies = args['policy']['suppression'] if 'suppression' in args['policy'] else {}
        self.cache = {}
        
    def init(self,**args):
        """
            This function is a live Initialization function i.e it will set the meta data of a table
            The meta data is of type SchemaField (google bigquery API reference)
            
            @param meta meta data list of SchemaField objects
        """
        self.meta = args['meta'] 
        
    #def can_do(self,id,meta):
        #"""
            #This function will determine if a table has fields that are candidate for suppression
            
            #@param id       table identifier
            #@param meta     list of SchemaField objects
        #"""
        #if id not in self.cache :
            #if id in self.policies and id not in self.cache:
                #info = self.policies[id]
                #self.cache[id] = sum([ int(field.name in info) for field in meta ]) > 0
            #else:
                #self.cache[id] = False
        #return self.cache[id]
    def get(self,id,info):
        """
            This function will return a row after suppressing fields that need to be suppressed
            @param id       table identifier
            @param info     meta data (information for a given table)
        """
        if id in self.cache :
            ref = self.policies[id]
            return [field for field in info if field.name not in ref]
        return None
    def do(self,**args):
        """
            This function will create a new table in a designated area. The function requires can_do function to be run before:
            This will set the cache with the appropriate information
            @pre:       self.can_do(table_name,meta) == True
            
            
            @param i_dataset   input/source dataset
            @param table_name     input/source table
            @param o_dataset    target dataset identifier
        """
        i_dataset       = args['i_dataset']
        table_name      = args['table_name']
        o_dataset       = args['o_dataset']
        
        job     = bq.QueryJobConfig()
        ref     = self.client.dataset(i_dataset).table(table_name)
        meta    = self.client.get_table(ref).schema
        fields  = self.get(table_name,meta)
        # 
        # setting up the destination
        otable = self.client.dataset(o_dataset).table(table_name)
        job.destination = otable
        fields = ",".join([field.name for field in fields])
        sql = """
                SELECT :fields
                FROM :table
        """.replace(":table",i_dataset+"."+table_name).replace(":fields",fields)
        self.client.query(
                sql,
                location='US',job_config=job)
        pass

class Transform(Policy):
    def __init__(self,**args):
        pass

class Shift(Transform):
    
    """
        This class is designed to determine how dates will be shifted. 
        i.e A value will be returned as follows {name:<name>,value:<value>} (this is a transformation)
    """
    def __init__(self,**args):
        
        pass
    def do(self,**args):
        """
        """
        table_name = ".".join([args['dataset'],args['table_name']])
        fields = self.cache[table_name]
        field_values = []
        OFFSET_TERM = "DATETIME_ADD(:field, INTERVAL :offset DAYS)"
        for field in fields :
            value = field+" = "+OFFSET_TERM.replace(":field",field).replace(":offset",self.offset)
        sql = """
            UPDATE :table_name
            set :field_values
        """.replace(":table_name",table_name).replace(":field_values",field_values)
        
    
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
            ltables = self.get_tables(dataset_name)
            print " ** ", len(ltables)
            table = [table for table in ltables if table.table_id == table_name ]
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
            
#handler = BQHandler('config/account/account.json')
#handler.meta('raw','concept')