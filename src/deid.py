"""
    AoUS - DEID, 2018
    Steve L. Nyemba<steve.l.nyemba@vanderbilt.edu>
    
    This file implements the deidentification policy for the All of Us project. This is a rule based approach:
        - Suppression of PPI, EHR, PM
        - Date Shifting
        - Generalization of certain fields (Zip, ICD9)
        
    It should be noted that the DRC database is hybrid rational & meta-database of sort (not sure why hybrid)
    The database will contain meta information about the data and the data as well (in most cases).
    More information on metamodeling https://en.wikipedia.org/wiki/Metamodeling
    
    Design:
    The core of the design consists in generating SQL statements that perform suppression, date-shifting and generalization.
    The queries will be compiled in an class who's role is to orchestrate the operations using joins and unions.
    e.g :
        If a table (relational) has a date-field and one or more arbitrary fields to be removed:
        1. A projection will be run against the list of fields that would work minus the date fields
        2. The date fields will be shifted given separate logic
        3. The result of (1) and (2) will be joined on person_id (hopefully we don't need to specify a key)

    e.g:
        If a meta table with a date field needs to be removed the above operation is performed twice:
        1. First on a subset of records filtered by records for any date type (specified as a concept)
        2. The second subset of records contains the dates (specified by concepts) and will be shifted (row based operation)
        3. The results of (1) and (2) will unioned. Note that (1) include a join already
        
    Once the Query is put together we send it to bigquery as a job that can be monitored

@TODO: Add logging to have visibility into what the code is doing 
    
    
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
    class TERMS :
        SEXUAL_ORIENTATION_STRAIGHT     = 'SexualOrientation_Straight'
        SEXUAL_ORIENTATION_NOT_STRAIGHT = 'SexualOrientation_None'
        OBSERVATION_FILTERS = {"race":'Race_WhatRace',"gender":'Gender',"orientation":'Orientation',"employment":'_EmploymentStatus',"sex_at_birth":'BiologicalSexAtBirth_SexAtBirth',"language":'Language_SpokenWrittenLanguage',"education":'EducationLevel_HighestGrade'}
    
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
                    #
                    # At this point we have to perform a join on relational date fields, the dates are determined by the date at which a given candidate signed up
                    #
                    sql = """
                        SELECT x.person_id,:fields 
                        FROM :i_dataset.observation x INNER JOIN 
                            :i_dataset.:table __targetTable

                        ON __targetTable.person_id = x.person_id 
                        WHERE x.observation_source_value = 'ExtraConsent_TodaysDate'
                        :additional_condition
                        
                    """.replace(":fields",sql_fields).replace(":i_dataset",dataset).replace(":table",table)
                    #
                    # @NOTE: If the working table is observation we should add an additional condition in the filter
                    # This would improve the joins performance
                    # @TODO: Find a way to re-write the query (simpler is better) and remove the condition below
                    #
                    
                    if table == 'observation' :
                        sql = sql.replace(":additional_condition"," AND x.observation_id = __targetTable.observation_id")
                    else:
                        sql = sql.replace(":additional_condition","")                    
                    self.policies[name] = {"join":{"sql":sql,"fields":joined_fields}}

                if q :
                   
                    #
                    # q == True implicitly means that self.cache[name] is True (propositional logic)
                    # We are dealing with a meta table (observation), We need to shift the dates of the observation_source_value that are of type date
                    #
                    # I assume the only meta-table is the observation table in the case of another one more parameters need to be passed
                    #
                    union_fields = ['value_as_string']+joined_fields +['person_id']
                
                    #--AND person_id = 562270
                    _sql = """
                    
                        SELECT CAST (DATE_DIFF(CAST(x.value_as_string AS DATE),CAST(y.value_as_string AS DATE),DAY) as STRING) as value_as_string, x.person_id, :shifted_fields :fields
                        FROM :i_dataset.observation x INNER JOIN (
                            SELECT MAX(value_as_string) as value_as_string, person_id
                            FROM :i_dataset.observation
                            WHERE observation_source_value = 'ExtraConsent_TodaysDate'
                            
                            GROUP BY person_id
                        ) y ON x.person_id = y.person_id 
                        
                        WHERE observation_source_value in (
                            
                            SELECT concept_code from :i_dataset.concept 
                            WHERE REGEXP_CONTAINS(concept_code,'(DATE|Date|date)') IS TRUE
                            
                        )
                         
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
        # self.fields = args['fields'] if 'fields' in args else []
        self.remove = args['remove'] if 'remove' in args else []
    def can_do(self,dataset,table):
        name = dataset+"."+table
        if name not in self.cache :
            try:
                ref     = self.client.dataset(dataset).table(table)
                schema  = self.client.get_table(ref).schema
                # self.fields += [field.name for field in schema if field.field_type in ['DATE','TIMESTAMP','DATETIME']]
                # self.fields = list(set(self.fields))    #-- removing duplicates from the list of fields
                # p = len(self.fields) > 0  
                self.remove += [field.name for field in schema if field.field_type in ['DATE','TIMESTAMP','DATETIME']]
                self.remove = list(set(self.remove))    #-- removing duplicates from the list of fields
                
                p = len(self.remove) > 0       #-- Do we have fields to remove (for physical tables)
                q = table in Policy.META_TABLES #-- Are we dealing with a meta table               
                self.cache[name] = p or q
                sql = """
                    SELECT :fields
                    FROM :i_dataset.:table
                """
                
                if p :
                    # _fields = [field.name for field in schema if field.name not in self.fields] #--fields that will be part of the projection
                    _fields = [field.name for field in schema if field.name not in self.remove] 
                    lfields = list(_fields)
                    _fields = ",".join(_fields)
                else:
                    _fields = "*"
                    lfields = [field.name for field in schema]
                if q :
                    #
                    # We are dealing with observation / meta table. Certain rows have to be removed due to the fact that it's a meta-table
                    #   - Date  because they will be shifted
                    #   - {Race,Gender,Ethnicity, Education, Employment, Language, Sexual Orientation} because they will be generalized
                    # As a result of filtering out the above fields, we need to run a cascading Unions of which each will have its dates shifted.
                    #
                    codes = "'"+"','".join(self.concept_class_id)+"'"
                    sql_filter = "Date|"+ "|".join(Policy.TERMS.OBSERVATION_FILTERS.values())
                    # filter = 'Date|Gender|Race|Ethnicity|Employment|Orientation|Education'
                    sql = sql + """

                        WHERE observation_source_concept_id in (
                            SELECT concept_id 
                            FROM :i_dataset.concept 
                            WHERE vocabulary_id = ':vocabulary_id' AND concept_class_id in (:code)
                            
                            AND REGEXP_CONTAINS(concept_code,'(:filter)') IS FALSE
                        )

                        
                    """.replace(":code",codes).replace(":vocabulary_id",self.vocabulary_id).replace(":filter",sql_filter)
                    #
                    #   We are now having to generalize rows that were filtered out (done in a loop)
                    #   These queries will be unioned in the end.
                    #
                    xsql = [sql]
                    args = {"client":self.client,"dataset":dataset,"table":table,"fields":_fields,"sql":"","concept_source_id":[],"vocabulary_id":"","concept_class_id":[]}
                    handler = Group(**args)
                    for key in Policy.TERMS.OBSERVATION_FILTERS :
                        
                        pointer = getattr(handler,key)
                        r = pointer()
                        
                        if len(r.keys()) > 0 :
                            ofields = [ r[fname] if fname in r else fname for fname in lfields]
                            
                            _sql_ = "SELECT :fields FROM :i_dataset.:table WHERE observation_source_concept_id in (SELECT concept_id FROM :i_dataset.concept WHERE REGEXP_CONTAINS(concept_code,'(?i):key')) "
                            _sql_ = _sql_.replace(":fields",",".join(ofields)).replace(":table",table).replace(":key",key).replace(":i_dataset",dataset)
                            
                            xsql.append( " UNION ALL "+_sql_ )
                            
                            
                    sql =  " SELECT :fields from (" +" ".join(xsql) +") GROUP BY :fields"
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
        This class performs generalization against the data-model on a given table
        The operations will apply on :
            - gender/sex
            - sexual orientation
            - race
            - education
            - employment
            - language
        This is an inherently inefficient operation as a result of the design of the database that unfortunately has redudancies and semantic ambiguity (alas)
        As such this code will proceed case by case

        @TODO: Talk with the database designers to improve the design otherwise this code may NOT scale nor be extensible
    """
    def __init__(self,**args):
        """
            @param path     either the path to the service account or an initialized instance of the client
            @param sql      sql query to execute
            @param dataset  dataset subject
            @param table    table (subject of the operation)
        """
        Policy.__init__(self,**args)
        self.sql        = args['sql']
        self.dataset    = args['dataset']
        self.table      = args['table']
        if isinstance(args['fields'],basestring) :
            self.fields = args['fields'].split(',')
        else:
            self.fields     = args['fields']

    def get_fields(self,p):
        """
            This function returns the field list with generalized expressions of the fields
            @param p    mapping parameter of fields and associated expressions
        """
        
        fields = list(self.fields)
        r = {}
        for name in p :
            
                     
            if name in fields:
                index = fields.index(name)    
                value = p[name]
                fields[index] = value
                r[name] = value
        # return fields
        
        return r
    def race(self):
        """
            let's generalize race as follows all non-{white,black,asian} should be grouped as Other
            @pre :
                requires concept table to exist and be populated.
        """
        #
        # For reasons I am unable to explain I noticed that the basic races were stored in concept table as integers
        # The goal of the excercise is that non {white,black,asians} are stored as others.
        # The person table has redundant information (not sure why) in race_concept_id and race_source_value
        #

        # @TODO: Multiple races (add this)
        
        #
        # We retrieve the identifiers of all the known races {black,white,asian,other} and anything that doesn't belong will be other
        # @NOTE:
        #   For some unknown reason (poor design) it would appear that on tables like person concept_name holds the value of the race whereas in observation table concept_code holds the value of the race
        # This is an unacceptable inconcsistency that make make data broadly available with different representations thus increasing the risk of re-identification.
        #
        
        field_name = "concept_name" if self.table == 'person' else 'concept_code'
        fields = self.fields 
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE REGEXP_CONTAINS(vocabulary_id,'(PPI|Race)') AND REGEXP_CONTAINS(concept_name,'(White|Black|Asian|Other Race)') is TRUE AND REGEXP_CONTAINS(concept_name,'(Native|Pacific)') is FALSE"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
        other_id= r[r['concept_name'] == 'Other Race']['concept_id'].tolist()[0]
        other_name= r[r['concept_name'] == 'Other Race']['concept_name'].tolist()[0]
        _ids    = [str(value) for value in r[r['concept_name'] != 'Other Race']['concept_id'].tolist()]
        #
        # Formatting the fields to perform the generalization of the  of the a person
        #
        _ids        = ",".join(_ids)
        other_id    = str(other_id)
        
        p       = {}
        if self.table == 'person' :
            
            p["race_concept_id"] = "IF(race_concept_id not in (:_ids),:other_id,race_concept_id) as race_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p["race_source_value"]="IF(race_concept_id not in (:_ids),':other_name',race_source_value) as race_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            
            # return self.get_fields(p)

        else:
            #
            # Let's generalize race and everything that goes with
            # @TODO: Figure out cases for multiple races
            p['value_as_string'] = "IF(value_source_concept_id not in (:_ids),':other_name',value_as_string) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name)
            p['observation_source_concept_id'] = "IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p['observation_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            p['value_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name)

           
        return self.get_fields(p)
        #
        # let's extract the other_id
        return None
    def gender(self):
        """
            This function will generalize gender from the person table as well as the observation table
            Other if not {M,F}            
            
            @NOTE : The table has fields with redundant information (shit design) i.e gender_source_value and gender_concept_id

            @param dataset
            @param table
            @param fields
        """
        sql = "SELECT concept_id,concept_name FROM :dataset.concept WHERE (vocabulary_id= 'Gender' AND concept_name not in ('FEMALE','MALE') ) OR REGEXP_CONTAINS(concept_code,'_Man|_Woman')"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
        
        other_id = str(r[r['concept_name']=='OTHER']['concept_id'].values[0])                        #--
        other_name = r[r['concept_name']=='OTHER']['concept_name'].values[0]                      #--
        _ids =",".join([str(value) for value in r[r['concept_name']!='OTHER']['concept_id'].tolist()])    #-- ids to generalize
        fields = self.fields #args['fields']
        
        p = {}
        if self.table == 'person' :
            #
            # We retrieve the identifiers of the fields to be generalized
            # The expectation is that we have {Male,Female,Other} with other having modern gender nomenclature
            #
            p ["gender_concept_id"] = "IF(gender_concept_id in ( :_ids ),:other_id,gender_concept_id) as gender_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p ["gender_source_value"]= "IF(gender_concept_id in (:_ids),:other_name,gender_source_value) as gender_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            # for name in p :
            #     index = fields.index(name)
            #     value = p[name].replace(":_ids",",".join(_ids)).replace(":other_id",str(other_id))
            #     if index > 0 :
            #         fields[index] = value
            # return fields
        else:
            #
            # This section will handle observations
            #
            p['value_as_string'] = "IF(value_source_concept_id not in (:_ids),':other_name',value_as_string) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name)
            p['observation_source_concept_id'] = "IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
            p['observation_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
            p['value_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        return self.get_fields(p)
    def __get_formatted_observations(self,_ids,other_name,other_id) :
       
        _ids = ",".join(_ids) if isinstance(_ids,list) else _ids        
        other_id = str(other_id) if isinstance(other_id,int) else other_id
        other_name = other_name.replace("'","\\'")
        p = {}
        p['value_as_string'] = "IF(value_source_concept_id not in (:_ids),':other_name',value_as_string) as value_as_string".replace(":_ids",_ids).replace(":other_name",other_name)
        p['observation_source_concept_id'] = "IF(value_source_concept_id not in (:_ids),:other_id,observation_source_concept_id) as observation_source_concept_id".replace(":_ids",_ids).replace(":other_id",other_id)
        p['observation_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',observation_source_value) as observation_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        p['value_source_value'] = "IF(value_source_concept_id not in (:_ids), ':other_name',value_source_value) as value_source_value".replace(":_ids",_ids).replace(":other_name",other_name)
        return self.get_fields(p)
    def ethnicity(self):
        """
            This function generalizes the ethnicity of an individual i.e 
            an ethnicity can be {hispanic or latino, not hispnaic or latino}
            @NOTE: This will be dropped !!
        """
        return None
    def orientation(self):
        """
            This function will generalize sexual orientation on the observation table, this only applies to the observation table (for now)
            @filter    TheBasics_SexualOrientation
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept where REGEXP_CONTAINS(concept_code, 'Orientation_Straight|Orientation_None')"
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()
       
        other_id = str(r[r['concept_code'] == Policy.TERMS.SEXUAL_ORIENTATION_NOT_STRAIGHT]['concept_id'].tolist()[0])                        #--
    
        other_name = r[r['concept_code']==Policy.TERMS.SEXUAL_ORIENTATION_NOT_STRAIGHT]['concept_code'].tolist()[0]  
        #                     #--
        _ids =[str(value) for value in r[r['concept_code'] ==Policy.TERMS.SEXUAL_ORIENTATION_STRAIGHT]['concept_id'].tolist()]    #-- ids to generalize
        
        fields = self.fields
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def education(self):
        """
            Educattion should be in 5 categories provided by the concept_codes below. Because we do NOT have an unknown education level we will hard code it and set it's concept id to zero (No matching concept)
            @TODO:
            The data curation team should add this in the concept table (put in a request with Mark or Chun Yee)
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE concept_code in ('HighestGrade_AdvancedDegree','HighestGrade_CollegeOnetoThree','HighestGrade_TwelveOrGED','HighestGrade_NeverAttended')"        
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def sex_at_birth(self):
        """
            This function will perform sex at birth generalization against the observation table
            @filter value_source_concept_id in (SELECT concept_id from :dataset.concept WHERE concept_code = 'BiologicalSexAtBirth_SexAtBirth')
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE  concept_code in ('SexAtBirth_Female', 'SexAtBirth_Male')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)

    def language(self):
        """
            filter by SpokenWrittenLanguage_
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE REGEXP_CONTAINS(concept_code,'Language_English')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
    def employment(self):
        """
            This function will generalize employment
            This will have to be filtered by _EmploymentStatus
        """
        sql = "SELECT concept_id,concept_code,concept_name from :dataset.concept WHERE concept_code in ('EmploymentStatus_OutOfWorkOneOrMore','EmploymentStatus_EmployedForWages','EmploymentStatus_OutOfWorkLessThanOne')"
        other_id = '0'
        other_name = 'Unknown'
        sql = sql.replace(":dataset",self.dataset)
        r = self.client.query(sql)
        r = r.to_dataframe()        
        _ids = [str(value) for value in r['concept_id'].tolist()]
        
        return self.__get_formatted_observations(_ids,other_name,other_id)
        
class Orchestrator():
    """
        This class is designed to run deidentification against an OMOP table/database provided configuration
        @param dataset
        @param table
        @param vocabulary_id
        @param concept_class_id
    """
    def __init__(self,**args):
        self.actors  = [Shift(**args),DropFields(**args)] #,Group(**args)]
        self.dataset = args['dataset'] 
        self.table   = args['table']
        self.sql     = None
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
                    sql = sql + " INNER JOIN (:sql) p ON p.person_id = a.person_id ".replace(":sql",join_sql)
                    #
                    # If we are dealing with observations we should tie down this record
                    #
                    # if 'observation_id' in fields :
                    #     sql += " "

                else:
                    join_fields = ""    
                sql = sql.replace(":joined_fields",join_fields)
                fields = [field.replace('a.','') for field in fields]
                if 'union' in r['shift']:
                    #
                    # we perform a union operation on this table in order to add meta data table information to the original projection
                    union_sql = r['shift']['union']['sql']
                    non_union_fields = list(set(fields) - set(r['shift']['union']['fields']))
                    non_union_fields = ",".join([' ']+non_union_fields)
                    union_sql = union_sql.replace(":fields",non_union_fields)
                    sql = sql + " UNION ALL SELECT :fields :joined_fields FROM ( :sql ) ".replace(":sql",union_sql)    
                    
                    sql = sql.replace(":fields",",".join(fields)).replace(":joined_fields",join_fields)
                    pass
                pass
            #
            # at this point we create a view that will serve as a basis for the shifting
            #   
            #
            self.sql = sql #"".join(["CREATE VIEW out.:table AS (",sql,")"])
            self.fields = list(set(fields + join_fields.split(",") ) - set(['']))
            # args = {}
            # args['client']  = client
            # args['sql']     = _sql
            # args['dataset'] = self.dataset
            # args['table']   = self.table
            # Group(**args)
    def generalize(self,**args):
        pass
            # print _sql #.replace(":fields",fields)
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

