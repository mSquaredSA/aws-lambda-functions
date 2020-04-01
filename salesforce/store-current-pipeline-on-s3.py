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
    
    
    #create connection to salesforce instance
    sf = Salesforce(username='************************',password='*******************',security_token='api_key')
    
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
    
    sf_current_pipeline = sf.query_all(current_pipeline_query)
    current_pipeline_df = pd.DataFrame(sf_current_pipeline['records'])
    
    
    today_dt = datetime.now(pytz.timezone('US/Eastern'))
    current_month = today_dt.month
    current_year = today_dt.year
    today_str = today_dt.strftime('%Y-%m-%d')
    
    competing_against_list_column = ['algolia','attraqt','bloomreach','celebros','doofinder','easyask','ecommfinder','endeca','fact_finder','findify','fusion_bot','google_commerce','google_onsite','hawk_search','home_grown','instantsearchplus','klevu','other','reflektion','richrelevance','searchanise','shoptivate','sli_systems','solr','sphinx_search','swiftype','thanx_media','unbxd']
    competing_against_list_value = ['Algolia','ATTRAQT','Bloomreach','Celebros','DooFinder','EasyAsk','eCommFinder','Endeca','Fact Finder','Findify','Fusion Bot','Google Commerce','Google Onsite','Hawk Search','Home Grown','InstantSearch+','Klevu','Other','Reflektion','RichRelevance','Searchanise','Shoptivate','SLI Systems','SOLR','Sphinx Search','Swiftype','Thanx Media','UNBXD']


    for competitor in competing_against_list_column:
        current_pipeline_df['competing_against_'+competitor] = None
        
    for num in (range(0,28)):    
    
        for index,item in enumerate(current_pipeline_df['Competing_Against__c']):
    
            try:
                if competing_against_list_value[num] in item:
                    current_pipeline_df['competing_against_'+competing_against_list_column[num]][index] = 1
    
                else:
                    current_pipeline_df['competing_against_'+competing_against_list_column[num]][index] = 0
    
    
            except TypeError:
    
                current_pipeline_df['competing_against_'+competing_against_list_column[num]][index] = 0
    
    current_pipeline_df.drop(labels='attributes',inplace=True,axis=1)
    current_pipeline_df['as_of_dt'] = today_str
    current_pipeline_df['CreatedDate'] = current_pipeline_df['CreatedDate'].apply(adjust_time)
    
    current_pipeline_df.rename(columns={
        
        'AccountId':'account_id',
        'Opportunity_ID__c':'opportunity_id',
        'Name':'name',
        'OwnerId':'owner_id',
        'CreatedDate':'create_date',
        'CloseDate':'close_date',
        'StageName':'stage',
        'Type':'type',
        'Recurring_Amount__c':'recurring_amount',
        'One_time_Amount__c':'one_time_amount',
        'Competing_Against__c':'competing_against',
        'Annual_Website_Sessions__c':'annual_website_sessions',
        'No_of_Queries__c':'no_of_queries',
        'No_of_SKUs__c':'no_of_skus',
        'LeadSource':'lead_source',
        'Lead_Source_Detail__c':'lead_source_detail',
        'Partner__c':'partner',
        'Partner_Commission_Opt_Out__c':'partner_commission_opt_out',
        'Partner_Endorsement__c':'partner_endorsement',
        'Partner_Referral_Method__c':'partner_referral_method',
        'Probability':'probability',
        'pandadoc__TrackingNumber__c':'panda_doc_tracking_number',
        
    },inplace=True)
    
    
    current_pipeline_df = current_pipeline_df[['as_of_dt','opportunity_id', 'name', 'owner_id','create_date', 'close_date', 'stage', 'type',
       'recurring_amount', 'one_time_amount', 'competing_against','annual_website_sessions','no_of_queries','no_of_skus',
       'lead_source', 'lead_source_detail', 'partner',
       'partner_commission_opt_out', 'partner_endorsement','account_id',
       'partner_referral_method', 'competing_against_algolia',
       'competing_against_attraqt', 'competing_against_bloomreach',
       'competing_against_celebros', 'competing_against_doofinder',
       'competing_against_easyask', 'competing_against_ecommfinder',
       'competing_against_endeca', 'competing_against_fact_finder',
       'competing_against_findify', 'competing_against_fusion_bot',
       'competing_against_google_commerce', 'competing_against_google_onsite',
       'competing_against_hawk_search', 'competing_against_home_grown',
       'competing_against_instantsearchplus', 'competing_against_klevu',
       'competing_against_other', 'competing_against_reflektion',
       'competing_against_richrelevance', 'competing_against_searchanise',
       'competing_against_shoptivate', 'competing_against_sli_systems',
       'competing_against_solr', 'competing_against_sphinx_search',
       'competing_against_swiftype', 'competing_against_thanx_media',
       'competing_against_unbxd','probability','panda_doc_tracking_number']]
    
    
    #upload current pipeline csv to S3
    with open('/tmp/'+today_str+'-sf-current-pipeline.csv', 'w+') as file:
        current_pipeline_df.to_csv('/tmp/'+today_str+'-sf-current-pipeline.csv',index=False,sep='|')
        file.close()

    s3_path = str(current_year)+'/'+str(current_month)+'/'+today_str+'-sf-current-pipeline.csv'

    s3 = boto3.client('s3')
    s3.upload_file('/tmp/'+today_str+'-sf-current-pipeline.csv','current-pipeline-daily-snapshots', s3_path)
    
    
