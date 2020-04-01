from simple_salesforce import Salesforce
import boto3
import pandas as pd
from datetime import date
from datetime import datetime
import pytz

def lambda_handler(event, context):
    
    #helper functions
    def adjust_time(createDate):
    
        split_date = createDate.split('.')[0]
        
        dt_date = datetime.strptime(split_date,'%Y-%m-%dT%H:%M:%S')
        
        final_date = dt_date.strftime('%Y-%m-%d')
        
        return final_date
        
    def batch_execute_statement(sql, sql_parameter_sets):
        response = rds.batch_execute_statement(
            secretArn=secret_arn,
            database=db_name,
            resourceArn=resource_arn,
            sql=sql,
            parameterSets=sql_parameter_sets
        )
        return response
    
    
    #create connection to salesforce instance
    sf = Salesforce(username='************************',password='*********************',security_token='api_key')
    
    #query and return current pipeline data from Salesforce
    current_pipeline_query = '''

        SELECT
            Opportunity_ID__c,
            Name,
            OwnerId,
            CreatedDate,
            CloseDate,
            StageName,
            Type,
            Recurring_Amount__c,
            One_time_Amount__c,
            Competing_Against__c,
            Annual_Website_Sessions__c,
            No_of_Queries__c,
            No_of_SKUs__c,
            LeadSource,
            Lead_Source_Detail__c,
            Partner__c,
            Partner_Commission_Opt_Out__c,
            Partner_Endorsement__c,
            Partner_Referral_Method__c,
            Probability,
            pandadoc__TrackingNumber__c,
            AccountId
        
        
        FROM Opportunity
        
        WHERE StageName NOT IN ('Closed Won','Closed Lost')
           
        '''
    
    today_dt = datetime.now(pytz.timezone('US/Eastern'))
    today_str = today_dt.strftime('%Y-%m-%d')
        
    sf_current_pipeline = sf.query_all(current_pipeline_query)
    current_pipeline_records = sf_current_pipeline['records']
    
    
    #clean query data and prepare reformat for transfer to rds instance
    cp_records_list = []
    competing_against_list_column = ['algolia','attraqt','bloomreach','celebros','doofinder','easyask','ecommfinder','endeca','fact_finder','findify','fusion_bot','google_commerce','google_onsite','hawk_search','home_grown','instantsearchplus','klevu','other','reflektion','richrelevance','searchanise','shoptivate','sli_systems','solr','sphinx_search','swiftype','thanx_media','unbxd']
    competing_against_list_value = ['Algolia','ATTRAQT','Bloomreach','Celebros','DooFinder','EasyAsk','eCommFinder','Endeca','Fact Finder','Findify','Fusion Bot','Google Commerce','Google Onsite','Hawk Search','Home Grown','InstantSearch+','Klevu','Other','Reflektion','RichRelevance','Searchanise','Shoptivate','SLI Systems','SOLR','Sphinx Search','Swiftype','Thanx Media','UNBXD']

        
    for output in current_pipeline_records:
        
        del output['attributes']
        output['as_of_dt'] = today_str
            
        for num in (range(0,28)):    
        
            try:
                if competing_against_list_value[num] in output['Competing_Against__c']:
                    output['competing_against_'+competing_against_list_column[num]] = 1
                else:
                    output['competing_against_'+competing_against_list_column[num]] = 0
    
    
            except TypeError:
                output['competing_against_'+competing_against_list_column[num]] = 0
            
        dict_output = dict(output)
        cp_records_list.append(dict_output)
 
 
 
    data_type_dict = {
        
        'Opportunity_ID__c':'stringValue',
        'Name':'stringValue',
        'OwnerId':'stringValue',
        'CreatedDate':'stringValue',
        'CloseDate':'stringValue',
        'StageName':'stringValue',
        'Type':'stringValue',
        'Recurring_Amount__c':'doubleValue',
        'One_time_Amount__c':'doubleValue',
        'Competing_Against__c':'stringValue',
        'Annual_Website_Sessions__c':'doubleValue',
        'No_of_Queries__c':'doubleValue',
        'No_of_SKUs__c':'doubleValue',
        'LeadSource':'stringValue',
        'Lead_Source_Detail__c':'stringValue',
        'Partner__c':'stringValue',
        'Partner_Commission_Opt_Out__c':'booleanValue',
        'Partner_Endorsement__c':'booleanValue',
        'Partner_Referral_Method__c':'stringValue',
        'Probability':'doubleValue',
        'pandadoc__TrackingNumber__c':'stringValue',
        'AccountId':'stringValue',
        'as_of_dt':'stringValue',
        'competing_against_algolia': 'longValue',
        'competing_against_attraqt': 'longValue',
        'competing_against_bloomreach': 'longValue',
        'competing_against_celebros': 'longValue',
        'competing_against_doofinder': 'longValue',
        'competing_against_easyask': 'longValue',
        'competing_against_ecommfinder': 'longValue',
        'competing_against_endeca': 'longValue',
        'competing_against_fact_finder': 'longValue',
        'competing_against_findify': 'longValue',
        'competing_against_fusion_bot': 'longValue',
        'competing_against_google_commerce': 'longValue',
        'competing_against_google_onsite': 'longValue',
        'competing_against_hawk_search': 'longValue',
        'competing_against_home_grown': 'longValue',
        'competing_against_instantsearchplus': 'longValue',
        'competing_against_klevu': 'longValue',
        'competing_against_other': 'longValue',
        'competing_against_reflektion': 'longValue',
        'competing_against_richrelevance': 'longValue',
        'competing_against_searchanise': 'longValue',
        'competing_against_shoptivate': 'longValue',
        'competing_against_sli_systems': 'longValue',
        'competing_against_solr': 'longValue',
        'competing_against_sphinx_search': 'longValue',
        'competing_against_swiftype': 'longValue',
        'competing_against_thanx_media': 'longValue',
        'competing_against_unbxd': 'longValue'
        
    }

    sql_parameter_sets = []
    
    for item in cp_records_list:
        
        dict_keys = item.keys()
        empty_list = []
        
        for key in dict_keys:
            empty_dict = {'name': '','value': None}
            empty_dict['name'] = key
            
            if item[key] is None:
                empty_dict['value'] = {'isNull': True}
            elif key == 'CreatedDate':
                empty_dict['value'] = {data_type_dict[key]: adjust_time(item[key])}
            else:
                empty_dict['value'] = {data_type_dict[key]: item[key]}
                
                
            empty_list.append(empty_dict)
            
        sql_parameter_sets.append(empty_list)
        
        
    
    #create sql query to insert records into rds instance
    values_string = ''

    for key in data_type_dict.keys():
        values_string = values_string + ':' + key + ','
        
    values_string = values_string[:-1]
    sql = 'INSERT INTO opps_current_pipeline VALUES' + ' (' + values_string + ')'
    
    
    #insert records into the rds instance using the helper function from above
    rds = boto3.client('rds-data')
    db_name = 'test_db'
    resource_arn = '*********************************'
    secret_arn = '**************************************************************************************************************'
    batch_execute_statement(sql,sql_parameter_sets)    
