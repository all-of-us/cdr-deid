"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
        - Suppression of PPI, EHR, PM
        - Date Shifting
        - Generalization of certain fields (Zip, ICD9)
        
    It should be noted that the DRC database is hybrid rational & meta-database of sort (not sure why hybrid)
    The database will contain meta information about the data and the data as well (in most cases).

    Design:
    The core of the design consists in generating SQL statements that perform suppression, date-shifting and generalization.
    The queries will be compiled in an class who's role is to orchestrate the operations using joins and unions.

    
    
"""
from __future__ import division
import json
from google.cloud import bigquery as bq

class Policy :
    """
        This function will apply Policies given the fields found on a given table
        The policy hierarchy will be applied as an iterator design pattern.
    """
    META_TABLES = ['observation']
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
        self.cache = {}
        if 'client' in args :
            self.client = args['client']
        elif 'path' in args :
            self.client = bq.Client.from_service_account_json(args['path'])
        self.vocabulary_id = args['vocabulary_id'] if 'vocabulary_id' in args else 'PPI'
        self.concept_class_id = args['concept_class_id'] if 'concept_class_id' in args else ['Question','PPI Modifier']
        if isinstance(self.concept_class_id,str):
            self.concept_class_id = self.concept_class_id.split(",")            

    def can_do(self,id,meta):
        return False
    def get(self,dataset,table) :
        return None
    def name(self):
        return self.__class__.__name__.lower()


class Shift (Policy):
    """
        This class will generate the SQL queries that will perform date-shifting against either a meta-table, relational table or a hybrid table.
        The way in which they are composed will be decided by the calling code that will serve as an "orchestrator". 
        for example:
            A hybrid table will perform the following operations given the nature of the fields:
                - For physical date fields a JOIN
                - For meta fields a UNION
                
        
    """
    def __init__(self,**args):
        Policy.__init__(self,**args)
        
        self.concept_sql = """
                SELECT concept_code from :dataset.concept
                WHERE vocabulary_id = ':vocabulary_id' AND REGEXP_CONTAINS(concept_code,'(Date|DATE|date)') is TRUE

            """
    def can_do(self,dataset,table):
        """
            This function determines if the a date shift is possible i.e :
            - The table has physical date fields 
            - The table has concept codes that are of date type
            @param table    table identifier
            @param dataset  dataset identifier
        """
        p = False
        q = False
        name = ".".join([dataset,table])
        if name not in self.cache :
            try:
                ref = self.client.dataset(dataset).table(table)
                info = self.client.get_table(ref).schema
                fields = [field for field in info if field.field_type in ('DATE','TIMESTAMP','DATETIME')]
                p = len(fields) > 0 #-- do we have physical fields as concepts
                q = table in Policy.META_TABLES
                sql_fields = self.__get_shifted_fields(fields)
                #
                # In the case we have something we should store it
                self.cache[name] = p or q
                joined_fields = [field.name for field in fields]
                if self.cache[name] == True :
                    # self.policies[name] = fields
                    # sql_fields = self.__get_shifted_fields(fields)
                    
                    # self.policies[name]["fields"] = f
                    sql = """
                        SELECT x.person_id,:fields 
                        FROM :i_dataset.observation x INNER JOIN 
                            :i_dataset.:table __targetTable

                        ON __targetTable.person_id = x.person_id 
                        WHERE x.observation_source_value = 'ExtraConsent_TodaysDate'
                        AND x.value_as_string = __targetTable.value_as_string 
                        AND x.person_id = 562270 
                    """.replace(":fields",sql_fields).replace(":i_dataset",dataset).replace(":table",table)
                    self.policies[name] = {"join":{"sql":sql,"fields":joined_fields}}
                
                if q :
                   
                    #
                    # q == True implicitly means that self.cache[name] is True (propositional logic)
                    # We are dealing with a meta table (observation), We need to shift the dates of the observation_source_value that are of type date
                    #
                    # I assume the only meta-table is the observation table in the case of another one more parameters need to be passed
                    #
                    union_fields = ['value_as_string']+joined_fields +['person_id']
                    
                    _sql = """
                    
                        SELECT CAST (DATE_DIFF(CAST(x.value_as_string AS DATE),CAST(y.value_as_string AS DATE),DAY) as STRING) as value_as_string, x.person_id, :shifted_fields :fields
                        FROM :i_dataset.observation x INNER JOIN (
                            SELECT MAX(value_as_string) as value_as_string, person_id
                            FROM :i_dataset.observation
                            WHERE observation_source_value = 'ExtraConsent_TodaysDate'
                            AND person_id = 562270
                            GROUP BY person_id
                        ) y ON x.person_id = y.person_id 
                        
                        WHERE observation_source_value in (
                            
                            SELECT concept_code from :i_dataset.concept 
                            WHERE REGEXP_CONTAINS(concept_code,'(DATE|Date|date)') IS TRUE
                            
                        )
                         AND x.person_id = 562270 
                    """.replace(":i_dataset",dataset).replace(":shifted_fields",sql_fields)
                    
                    self.policies[name]["union"] = {"sql":_sql.replace('__targetTable.',''),"fields":union_fields}
                    
                    # self.policies[name]['meta'] = 'foo'
                #
                # @TODO: Log the results of the propositional logic operation (summarized)
                
            except Exception, e:
                # @TODO
                # We need to log this stuff ...
                print e
        
        return self.cache[name]
    def __get_shifted_fields(self,fields):
        """
            @param fields   a list of SchemaField objects
        """
        r = []
        for field in fields :
            shifted_field =  """
                DATE_DIFF( CAST(x.value_as_string AS DATE), CAST(__targetTable.:name AS DATE), DAY) as :name
            """.replace(':name',field.name)
            
            r.append(shifted_field)
           
        return ",".join(r)
    def get(self,dataset,table):
        """
        @pre:
            can_do(dataset,table) == True

            This function will return the sql queries for for either physical fields and meta fields
            @param dataset  name of the dataset
            @param table    name of the table
        """
        name = dataset+"."+table
        
        return self.policies[name] if name in self.policies else None

class DropFields(Policy):
    """
        This class generate the SQL that will perform suppression against either a physical table and meta-table        
        By default this class will suppress all of the datefields and other fields specified by the user. 
        This will apply to both relational and meta-tables
    """
    def __init__(self,**args):
        """
            @param vocabulary_id        vocabulary identifier by default PPI
            @param concept_class_id     identifier of the category of the concept by default ['PPI', 'PPI Modifier']
            @param fields   list of fields that need to be dropped/suppressed from the database
        """
        Policy.__init__(self,**args)
        self.fields = args['fields'] if 'fields' in args else []
    def can_do(self,dataset,table):
        name = dataset+"."+table
        if name not in self.cache :
            try:
                ref     = self.client.dataset(dataset).table(table)
                schema  = self.client.get_table(ref).schema
                self.fields += [field.name for field in schema if field.field_type in ['DATE','TIMESTAMP','DATETIME']]
                self.fields = list(set(self.fields))    #-- removing duplicates from the list of fields
                p = len(self.fields) > 0        #-- Do we have fields to remove (for physical tables)
                q = table in Policy.META_TABLES #-- Are we dealing with a meta table
                self.cache[name] = p or q
                sql = """
                    SELECT :fields
                    FROM :i_dataset.:table
                """
                
                if p :
                    _fields = [field.name for field in schema if field.name not in self.fields] #--fields that will be part of the projection
                    lfields = list(_fields)
                    _fields = ",".join(_fields)
                else:
                    _fields = "*"
                    lfields = [field.name for field in schema]
                if q :
                    codes = "'"+"','".join(self.concept_class_id)+"'"
                    sql = sql + """

                        WHERE observation_source_value in (
                            SELECT concept_code 
                            FROM :i_dataset.concept 
                            WHERE vocabulary_id = ':vocabulary_id' AND concept_class_id in (:code)
                            AND REGEXP_CONTAINS(concept_code,'(Date|date|DATE)') IS FALSE

                        )
                        AND person_id = 562270 
                    """.replace(":code",codes).replace(":vocabulary_id",self.vocabulary_id)

                sql = sql.replace(":fields",_fields).replace(":i_dataset",dataset).replace(":table",table)
                self.policies[name] = {"sql":sql,"fields":lfields}
               
                
        
            except Exception,e:
                print e
        return self.cache [name]
    def get(self,dataset,table):
        name = dataset+"."+table
        return self.policies[name] if name in self.policies else False
class Group(Policy):
    """
        This class implements a form of generalization by grouping values for specified fields
    """
    pass
class Orchestrator():
    """
        This class is designed to run deidentification against an OMOP table/database provided configuration
        @param dataset
        @param table
        @param vocabulary_id
        @param concept_class_id
    """
    def __init__(self,**args):
        self.actors  = [Shift(**args),DropFields(**args),Group(**args)]
        self.dataset = args['dataset'] 
        self.table   = args['table']
        # self.parent_fields = args['parent_fields'] if 'parent_fields' in args else None
        self.setup()
    def setup(self):
        """
            This function will setup the order of execution of deidentification operations/actors
            For example
        """
        r       = {}
        for item in self.actors :
            name    = item.name()
            p       =  item.can_do(self.dataset,self.table)
            # print [name,self.dataset+'.'+self.table,p]
            if p :
                r[name] = item.get(self.dataset,self.table)
            else:
                continue
       
        if 'dropfields' in r :
        #     print r['dropfields']['rel']
            fields =  r['dropfields']['fields']

            sql = "SELECT :parent_fields FROM ("+r['dropfields']['sql']+") a"
            # sql = sql.replace(":fields",top_prefixed_fields)
            # sql = sql.replace(":fields",",".join(fields))
            if 'shift' in r :
                if 'join' in r['shift'] :                    
                    # ",".join(["a."+field for field in fields])
                    #
                    # considering a cartesian product is performed, this will change the number of fields of the final output
                    # We therefore need to prefix the parts of the query appropriately

                    #
                    #
                    
                    
                    part_a_prefix = ['a.'+name for name in fields if name not in r['shift']['join']['fields']]
                    
                    part_a_prefix += r['shift']['join']['fields']
                    
                    top_prefixed_fields = ",".join(part_a_prefix)
                    sql = sql.replace(":parent_fields",top_prefixed_fields)
                    join_sql = r['shift']['join']['sql']
                    join_fields = ",".join(['']+r['shift']['join']['fields']) #-- should start with comma
                    sql = sql + " INNER JOIN (:sql) p ON p.person_id  = a.person_id".replace(":sql",join_sql)

                else:
                    join_fields = ""    
                sql = sql.replace(":joined_fields",join_fields)
                fields = [field.replace('a.','') for field in fields]
                if 'union' in r['shift']:
                    union_sql = r['shift']['union']['sql']
                    non_union_fields = list(set(fields) - set(r['shift']['union']['fields']))
                    non_union_fields = ",".join([' ']+non_union_fields)
                    union_sql = union_sql.replace(":fields",non_union_fields)
                    sql = sql + " UNION ALL SELECT :fields :joined_fields FROM ( :sql ) ".replace(":sql",union_sql)
                    # sql = union_sql
                    sql = sql.replace(":fields",",".join(fields)).replace(":joined_fields",join_fields)
                    
                pass
            
            print sql #.replace(":fields",fields)
        # if 'dropfields' in r :
        #     fields = ",".join(r['dropfields']['fields'])
        #     sql = "SELECT :fields FROM (:sql)".replace(":sql",r['dropfields']['sql']).replace(":fields", fields)
        #     if 'shift' in r :
              
        #         sql = " SELECT * FROM (:sql) a ".replace(":sql",r['shift']['rel'])
        #         if 'meta' in r['shift'] :
        #             sql = sql + " INNER JOIN (:sql) b ON a.person_id = b.person_id".replace(":sql",r['shift']['rel'])
        #         # sql = sql + " UNION (:sql)".replace(":sql",r['shift']['meta'])
            # print sql.replace(":fields",fields)


            
#handler = BQHandler('config/account/account.json')
#handler.meta('raw','concept')
# client = bq.Client.from_service_account_json('/home/steve/git/rdc/deid/config/account/account.json')

# h = Shift(client=client,vocabulary_id='PPI',concept_class_id=['Question','PPI Modifier'])
# [h.vocabulary_id,h.concept_sql]
# r = h.can_do('raw','observation')
# print h.get('raw','observation')['fields']

# h = Orchestrator(client=client,vocabulary_id='PPI',concept_class_id=['Question','PPI Modifier'],dataset='raw',table='observation',fields=['value_as_number'])

