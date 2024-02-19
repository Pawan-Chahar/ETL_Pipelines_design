
        
        
          
create  or alter      procedure usp_REGISTRATION_MERGING          
          
as          
BEGIN          
      
 Declare @Insert_Date date=NULL;             
set @Insert_Date= (select top 1 Insert_Date  from GST_IMPL_NEW_TEST.[dbo].[DEALER_REGISTARTION] group by INSERT_DATE order by Insert_Date desc);         
          
--alter table [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] add [CIRCLE_CODE] varchar(50)          
--alter table [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] add [CENTER_JURISDICTION_CODE] varchar(50)          
--alter table [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] add [RISK_PROFILE] varchar(50)          
--alter table [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] add [ADDRESS] varchar(max)          
--alter table [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] add [PAN_NO] varchar(50)          
     
  
select [GSTIN]          
   ,[TRADE_NAME]          
   ,[LEGAL_NAME]          
   ,[REG_TYPE]          
   ,[STATUS_TYPE]          
   ,[DT_REG]          
   ,[DT_DEREG]          
   ,[JURISDICTION]          
   ,[CIRCLE]          
   ,[DIVISION]          
   ,[MOBILE_NO]          
   ,[EMAIL]          
   ,[NTBZ]          
   ,[NTBZ_DESCRIPTION]          
   ,[NTBZ_PRIORITY]          
   ,[NTBZ_ALL]          
   ,[NTBZ_DESCRIPTION_ALL]          
   ,[COBZ_CD]          
   ,[INSERT_DATE]          
   ,[CIRCLE_CODE]          
   ,[CENTER_JURISDICTION_CODE]          
   ,[RISK_PROFILE]          
   ,[ADDRESS]          
   ,[PAN_NO]                                
into #DEALER_REGISTARTION_API from STAGING_GST_IMPL_API.[dbo].[DEALER_REGISTARTION_API] where INSERT_Date> @Insert_Date        
          
          
MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[DEALER_REGISTARTION] AS Target            
USING #DEALER_REGISTARTION_API AS Source             
ON Target.GSTIN = Source.GSTIN            
WHEN NOT MATCHED BY TARGET THEN            
    INSERT (          
    [GSTIN]          
   ,[TRADE_NAME]          
   ,[LEGAL_NAME]          
   ,[REG_TYPE]          
   ,[STATUS_TYPE]          
   ,[DT_REG]          
   ,[DT_DEREG]          
   ,[JURISDICTION]          
   ,[CIRCLE]          
   ,[DIVISION]          
   ,[MOBILE_NO]          
   ,[EMAIL]          
   ,[NTBZ]          
   ,[NTBZ_DESCRIPTION]          
   ,[NTBZ_PRIORITY]          
   ,[NTBZ_ALL]          
   ,[NTBZ_DESCRIPTION_ALL]          
   ,[COBZ_CD]          
   ,[INSERT_DATE]          
   ,[CIRCLE_CODE]          
   ,[CENTER_JURISDICTION_CODE]          
   ,[RISK_PROFILE]          
   ,[ADDRESS]          
   ,[PAN_NO]          
   )          
   values          
   (          
    Source.[GSTIN]          
   ,Source.[TRADE_NAME]          
   ,Source.[LEGAL_NAME]          
   ,Source.[REG_TYPE]          
   ,Source.[STATUS_TYPE]          
   ,Source.[DT_REG]          
   ,Source.[DT_DEREG]          
   ,Source.[JURISDICTION]          
   ,Source.[CIRCLE]          
   ,Source.[DIVISION]          
   ,Source.[MOBILE_NO]          
   ,Source.[EMAIL]          
   ,Source.[NTBZ]          
   ,Source.[NTBZ_DESCRIPTION]          
   ,Source.[NTBZ_PRIORITY]          
   ,Source.[NTBZ_ALL]          
   ,Source.[NTBZ_DESCRIPTION_ALL]          
   ,Source.[COBZ_CD]          
   ,Source.[INSERT_DATE]          
   ,Source.[CIRCLE_CODE]          
   ,Source.[CENTER_JURISDICTION_CODE]          
   ,Source.[RISK_PROFILE]          
   ,Source.[ADDRESS]          
   ,Source.[PAN_NO]          
   ) ;          
          
   
   
 select [GSTIN]          
   ,[STATE_CODE]          
   ,[BANK_NAME]          
   ,[IFSC_CODE]          
   ,[BANK_ADDRESS]          
   ,[ACCOUNT_NO]          
   ,[INSERT_DATE]                                 
into #DEALER_BANK_DETAILS_API from STAGING_GST_IMPL_API.[dbo].[DEALER_BANK_DETAILS_API] where INSERT_Date> @Insert_Date    
          
          
 MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[DEALER_BANK_DETAILS] AS Target            
USING #DEALER_BANK_DETAILS_API AS Source             
ON Target.GSTIN = Source.GSTIN and  Target.[ACCOUNT_NO] = Source.[ACCOUNT_NO]          
WHEN NOT MATCHED BY TARGET THEN            
    INSERT (          
   [GSTIN]          
   ,[STATE_CODE]          
   ,[BANK_NAME]          
   ,[IFSC_CODE]          
   ,[BANK_ADDRESS]          
   ,[ACCOUNT_NO]          
   ,[INSERT_DATE]          
   )          
   values          
   (          
    Source.[GSTIN]          
   ,Source.[STATE_CODE]          
   ,Source.[BANK_NAME]          
   ,Source.[IFSC_CODE]          
   ,Source.[BANK_ADDRESS]          
   ,Source.[ACCOUNT_NO]          
   ,Source.[INSERT_DATE]          
   ) ;          
          
   
   
  select [GSTIN]          
 ,[PIN_CODE]          
 ,[OWNER_COUNT]          
 ,[OWNER_NAME]          
 ,[PAN_NO]          
 ,[MOBILE_NO]          
 ,[EMAIL_ID]          
 ,[DESIGNATION]          
 ,[DATE_OF_BIRTH]          
 ,[INSERT_DATE]          
 ,DTYPE        
 ,IS_PRIMARY        
 ,ADDRESS                                      
into #AUTH_SIGNATORY_DETAILS_API from STAGING_GST_IMPL_API.[dbo].[AUTH_SIGNATORY_DETAILS_API] where INSERT_Date> @Insert_Date           
          
          
   MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[AUTH_SIGNATORY_DETAILS] AS Target            
USING #AUTH_SIGNATORY_DETAILS_API AS Source             
ON Target.GSTIN = Source.GSTIN            
WHEN NOT MATCHED BY TARGET THEN            
    INSERT (          
  [GSTIN]          
 ,[PIN_CODE]          
 ,[OWNER_COUNT]          
 ,[OWNER_NAME]          
 ,[PAN_NO]          
 ,[MOBILE_NO]          
 ,[EMAIL_ID]          
 ,[DESIGNATION]          
 ,[DATE_OF_BIRTH]          
 ,[INSERT_DATE]          
 ,DTYPE        
 ,IS_PRIMARY        
 ,ADDRESS        
 )          
 VALUES          
 (          
  Source.[GSTIN]          
 ,Source.[PIN_CODE]          
 ,Source.[OWNER_COUNT]          
 ,Source.[OWNER_NAME]          
 ,Source.[PAN_NO]          
 ,Source.[MOBILE_NO]          
 ,Source.[EMAIL_ID]          
 ,Source.[DESIGNATION]          
 ,Source.[DATE_OF_BIRTH]          
 ,Source.[INSERT_DATE]          
 ,Source.DTYPE        
 ,Source.IS_PRIMARY        
 ,Source.ADDRESS        
 ) ;          
          
          
   
   select [GSTIN]          
 ,[ADDRESS]          
 ,[PINCODE]          
 ,[LATITUDE]          
 ,[LONGITUDE]          
 ,[INSERT_DATE]                                          
into #GSTN_ADDRESS_API from STAGING_GST_IMPL_API.[dbo].[GSTN_ADDRESS_API] where INSERT_Date> @Insert_Date      
    
          
    MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[GSTN_ADDRESS] AS Target            
USING #GSTN_ADDRESS_API AS Source             
ON Target.GSTIN = Source.GSTIN            
WHEN NOT MATCHED BY TARGET THEN            
    INSERT           
 (          
   [GSTIN]          
 ,[ADDRESS]          
 ,[PINCODE]          
 ,[LATITUDE]          
 ,[LONGITUDE]          
 ,[INSERT_DATE]          
 )          
 values (  Source.[GSTIN]          
    ,Source.[ADDRESS]          
    ,Source.[PINCODE]          
    ,Source.[LATITUDE]          
    ,Source.[LONGITUDE]          
    ,Source.[INSERT_DATE]) ;          
          
    
    
   select [GSTIN]          
 ,[SECTOR_OF_BUSINESS]          
 ,[INSERT_DATE]                                             
into #DEALER_SECTOR_BUSINESS_API from STAGING_GST_IMPL_API.[dbo].[DEALER_SECTOR_BUSINESS_API] where INSERT_Date> @Insert_Date      
    
    
          
    MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[DEALER_SECTOR_BUSINESS] AS Target            
USING #DEALER_SECTOR_BUSINESS_API AS Source             
ON Target.GSTIN = Source.GSTIN            
WHEN NOT MATCHED BY TARGET THEN            
    INSERT           
 (          
   [GSTIN]          
 ,[SECTOR_OF_BUSINESS]          
 ,[INSERT_DATE]          
 )          
 values          
 (          
  Source.[GSTIN]          
 ,Source.[SECTOR_OF_BUSINESS]          
 ,Source.[INSERT_DATE]          
 ) ;          
          
          
   
   
    
          
          
          
    MERGE INTO [GST_IMPL_NEW_TEST].[dbo].[DEALER_HSN_MASTER] AS Target            
USING STAGING_GST_IMPL_API.[dbo].[DEALER_HSN_MASTER_API] AS Source             
ON Target.GSTIN = Source.GSTIN            
WHEN NOT MATCHED BY TARGET THEN            
    INSERT           
 (          
  [GSTIN]          
 ,[HSN]          
 ,[FREQUENCY]          
 ,[CHAPTER_COUNT]          
 ,[MAX_CHAPTER_FREQUENCY]          
 ,[IS_PRINCIPAN_CHAPTER]          
 )          
 VALUES          
 (          
  Source.[GSTIN]          
 ,Source.[HSN]          
 ,Source.[FREQUENCY]          
 ,Source.[CHAPTER_COUNT]          
 ,Source.[MAX_CHAPTER_FREQUENCY]          
 ,Source.[IS_PRINCIPAN_CHAPTER]          
 ) ;          
          
  
drop table #DEALER_REGISTARTION_API ;  
drop table  #DEALER_BANK_DETAILS_API ;  
drop table #DEALER_SECTOR_BUSINESS_API ;  
drop table #GSTN_ADDRESS_API ;  
drop table #AUTH_SIGNATORY_DETAILS_API ;  
  
end ;          
          
 --alter table [dbo].[DEALER_HSN_MASTER_API]  add primary key ([GSTIN] )          
          
          
 --exec usp_REGISTRATION_MERGING          
        
        
        
        
        
        
        
        
