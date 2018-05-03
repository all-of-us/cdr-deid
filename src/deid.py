"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
        - Suppression of PPI, EHR, PM
        - Date Shifting
        - Generalization of certain fields (Zip, ICD9)
        
    It should be noted that the DRC database is hybrid rational & meta-database of sort (not sure why hybrid)
    The database will contain meta information about the data and the data as well (in most cases).
    
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
    def instance(**args):
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
        This class is intended to abstract the operations associated with suppression i.e :
        - PPI   This can be fully performed within the database
        - EHR   This operation is performed given a list of fields and applies to a relational table.
        - PM    
        
    """
    def __init__(self,**args):
            Policy.__init__(self,**args)
            self.cache = {}
class SuppressPPI(Suppress):            
    """
        This function will perform suppression of PPI 
        DESIGN:
            - Retrieve the appropriate concepts given the vocabulary_id = 'PPI' 
            and insure that this applies to concept_class_id in ('Questions', 'PPI Modifier')
            
            @param <path|client>    provide an initialized client object or the path of a json service account
    """
    def __init__(self,**args):
        """
            @param vocabulary_id    vocabulary identifier by default 'PPI'
            @param concept_class_id concept_class_id by default ('Question','PPI Modifier')

        """
            
        Suppress.__init__(self,**args);
        self.vocabulary_id = args['vocabulary_id'] if 'vocabulary_id' in args else 'PPI'
        self.concept_class_id = args['concept_class_id'] if 'concept_class_id' in args else ['Question','PPI Modifier']
        if isinstance(self.concept_class_id,str):
            self.concept_class_id = self.concept_class_id.split(",")
        self.concept_sql = """
                SELECT concept_code from :dataset.concept
                WHERE vocabulary_id = ':vocabulary_id' AND REGEXP_CONTAINS(concept_code,'(Date|DATE|date)') is FALSE

            """
    def init(self,id):
        pass
    def can_do(self,id,meta=None):
        """
            This function determines if it can perform a suppression of PPI. The PPI extracted shouldn't be dates because dates will be shifted.
            @param id   vocabulary_id
            
        """
        if self.vocabulary_id not in self.cache :
            self.concept_sql = self.concept_sql.replace(":dataset",id).replace(":vocabulary_id",self.vocabulary_id)
            
            if self.concept_class_id is not None or len(self.concept_class_id) > 0 :
                #
                # In case we have concept_class_id specified we need to add a condition to the filter
                #
                self.concept_sql = self.concept_sql + " and concept_class_id in ('"+"','".join(self.concept_class_id)+"')"
            self.concept_sql = self.concept_sql +" GROUP BY concept_code"
            #
            # Execute the query to get a list of concepts that should be removed from a meta table
            #
            
            job = self.client.query(self.concept_sql,location='US')
            if job.error_result is None :
                self.policies[self.vocabulary_id] = list(job) 
            #
            # If no error is returned then it means we can perform this operation
            #
            self.cache[self.vocabulary_id] = job.error_result is None

        return self.cache[self.vocabulary_id] if self.vocabulary_id in self.cache else False
    
    def get(self,id,field=None):
        """
            returns the fields for a given
        """
        if id in self.policies :
            return self.policies[id]
        return None
    def do(self,**args):
        """
            This function performs supporession of PPI
        """
        i_dataset = args['i_dataset']
        o_dataset = args['o_dataset']
        table_name = args['table_name']
        vocabulary_id = self.vocabulary_id
        if self.can_do(i_dataset) : 
            #
            # @TODO: Log what is happening here and REMOVE THE PERSON_ID
            codes = self.get(self.vocabulary_id)
            print [len(codes),'Concept Will be suprressed from ', i_dataset,'.',table_name, ' to ' , o_dataset]
            codes = "'"+"','".join([item.concept_code for item in codes])+"'"
            
            sql = """
                
                SELECT *
                FROM :i_dataset.:table_name 
                WHERE observation_source_value not in (:concept_codes) and person_id = 562270
            """.replace(":i_dataset",i_dataset).replace(":table_name",table_name)
            sql = sql.replace(":concept_codes",codes)
            #
            # @TODO: Log what is happening here (setting up job)
            job = bq.QueryJobConfig()
            otable = self.client.dataset(o_dataset).table(table_name)
            job.destination = otable
            
            job = self.client.query(sql,location='US',job_config=job)
            #
            # @TODO Log job.error_result :
            #   if None, then no error occurred
            #   else    Log the error
            

class SuppressEHR(Suppress):
    """
        This class implements supression rules on a table (specified by meta data)
        
    """
    def __init__(self,**args):
        """
            This function is initialize the suppression of an EHR tables. The assumption is that EHR tables are rational and we can remove the fields
            @TODO: Make sure it is ok to remove the concepts instead of the table's physical attributes.
            
            @param supress      {schema_name:{table_o:[field_1,field_2],table_1:[]}}
            @param client|path  path to service account json file or client object
        """
        Policy.__init__(self,**args)
        
        self.policies = args['config'] if 'config' in args else {}
        self.cache = {}
        
    def init(self,**args):
        """
            This function is a live Initialization function i.e it will set the meta data of a table
            The meta data is of type SchemaField (google bigquery API reference)
            
        """
        pass
    def can_do(self,dataset,table):
        """
            This function determines if the class can suppress a given table.
            @param dataset  dataset identifier
            @param  table   table identifier
        """
        if dataset not in self.cache :
            self.cache[dataset] = {}
        if table not in self.cache[dataset] :
            ref = self.client.dataset(dataset).table(table)
            try:
                schema = self.client.get_table(ref).schema
                if len(schema) > 0 :
                    lfields = self.policies[dataset][table]
                    self.cache[dataset][table] = [field.name for field in schema if field.name not in lfields]
                
            except Exception,e:
                #
                # @TODO:
                # We need to log the error here
                pass
                
        return len(self.cache[dataset][table])>0 if dataset in self.cache and table in self.cache[dataset] else False
    def get(self,dataset, table):
        """
            This function will return a list of fields that need to be removed from a table
            @param id       table identifier
            @param info     meta data (information for a given table)
        """
        if dataset in self.cache and table in self.cache[dataset]:
            return self.cache[dataset][table]
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
        fields  = self.get(i_dataset,table_name)
        # 
        # setting up the destination
        otable = self.client.dataset(o_dataset).table(table_name)
        job.destination = otable
        fields = ",".join(fields)
        sql = """
                SELECT :fields
                FROM :table
        """.replace(":table",i_dataset+"."+table_name).replace(":fields",fields)
        job = self.client.query(
                sql,
                location='US',job_config=job)
        pass

class Shift(SuppressPPI):
    
    """
        This class is designed to determine how dates will be shifted. 
        i.e A value will be returned as follows {name:<name>,value:<value>} (this is a transformation)
        
        @TODO:
        The date shifting should be performed on the basis of a person's day of participation in the study
    """
    def __init__(self,**args):
        SuppressPPI.__init__(self,**args)
        self.concept_sql = """        
            SELECT concept_code from :dataset.concept
            WHERE vocabulary_id = ':vocabulary_id' AND REGEXP_CONTAINS(concept_code,'(Date|DATE|date)') is TRUE

        """      
        pass
    def do(self,**args):
        """
            This function will 
        """
        i_dataset   = args['i_dataset']
        o_dataset   = args['o_dataset']
        table_name  = args['table_name']
        print  " *** done "
        table_ref   = self.client.dataset(o_dataset).table(table_name)
        schema      = self.client.get_table(table_ref).schema
        fields      = ['x.'+item.name for item in schema if item.name not in ['observation_source_value','value_as_string']]
        fields      = ",".join(fields)
        sql = """

            SELECT :fields, x.observation_source_value, DATE_DIFF(cast(x.value_as_string as date), cast(y.value_as_string as date), DAY) as value_as_string
            FROM raw.observation   x INNER JOIN (SELECT value_as_string,person_id from raw.observation where observation_source_value = 'ExtraConsent_TodaysDate') y ON y.person_id = x.person_id

            WHERE x.person_id = 562270 AND
                x.observation_source_value in (
                select concept_code from raw.concept
                where
                vocabulary_id = ':vocabulary_id' AND REGEXP_CONTAINS(concept_code,'(date|Date|DATE)')
                :more
            )
        """.replace(':vocabulary_id',self.vocabulary_id).replace(':fields',fields)
        if self.concept_class_id is not None and len(self.concept_class_id) > 0 :
            # codes = self.get(self.vocabulary_id)
            codes = "'"+"','".join(self.concept_class_id)+"'"
            codes =  " AND concept_class_id in (:codes)".replace(":codes",codes)
        else:
            codes = ""
        sql = sql.replace(":more",codes)
        print sql
        job     = bq.QueryJobConfig()
        otable = self.client.dataset(o_dataset).table(table_name)
        job.destination = otable
        # job = self.client.query(
        #         sql,
        #         location='US',job_config=job)    
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