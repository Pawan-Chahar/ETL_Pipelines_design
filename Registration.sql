
    
    
                  
                        
                           
                                                              
                                      
CREATE       or alter                 PROCEDURE [dbo].[usp_Registration_API_Batch_Ingestion]                                      
@INSERT_DATE VARCHAR(20)                                      
AS                                      
BEGIN                                      
       SET ansi_warnings OFF        ----Handle error (String or Binary data will be truncated)                         
                     
    update TEST_MPCTD.[dbo].[TBL_REG_REGULAR_TAX_PAYER]              
    set CANCEL_DATE = null              
    where len(CANCEL_DATE ) <2  and CONVERT(DATE ,INSERTED_DATE,103) = '2023-11-15'              

--------------INCREMENTAL_LOAD_REFRESH_LOGIC-------------------------------------------------
                                   
--  Declare @Insert_Date date=NULL;             
--set @Insert_Date= (select top 1 Insert_Date  from GST_IMPL_NEW_TEST.[dbo].[DEALER_REGISTARTION] group by INSERT_DATE order by Insert_Date desc); 

                                     
-----DEALAER BANK DETAILS                                       
                                      
INSERT INTO [STAGING_GST_IMPL_API].[dbo].[DEALER_BANK_DETAILS_API]                                      
(                                      
       [GSTIN]                                      
      ,[STATE_CODE]                                      
      ,[BANK_NAME]                                      
      ,[IFSC_CODE]                                      
      ,[BANK_ADDRESS]                                      
      ,[ACCOUNT_NO]                                      
      ,[INSERT_DATE]                                      
)                                      
SELECT                                        
      [GSTIN]                                      
   ,[STATE_CODE]                                      
   ,UPPER([BANK_NAME]) [BANK_NAME]                                      
      ,UPPER([IFSC] )  [IFSC_CODE]                                      
      ,UPPER([BRANCH_ADD] ) [BANK_ADDRESS]                                      
      , ACC_NO   [ACCOUNT_NO]                                      
      ,[INSERTED_DATE]                                      
  FROM [TEST_MPCTD].[dbo].[TBL_REG_BANK_DETAILS] A                                     
   WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_BANK_DETAILS_API]  B                        
     WHERE  A.[GSTIN] = B.[GSTIN] COLLATE Latin1_General_CI_AI                           
     AND  A.ACC_NO = B.[ACCOUNT_NO]  COLLATE Latin1_General_CI_AI)                        
  AND CONVERT(DATE ,INSERTED_DATE,103) >=  @INSERT_DATE                                
                                      
                                      
  ----AUTH_SIGNATORY_DETAILS_API                                       
                          
                               
 ; WITH AUTH_SIGN AS                              
   (                          
                       
        SELECT F.GSTIN ,   [OWNER_NAME] ,  [PAN_NO]  ,                                
      UPPER(REPLACE(REPLACE(F.[8] ,'~',' '),'NULL' ,''))  PINCODE ,[MOBILE_NO] ,[EMAIL_ID]                          
     ,DEsignation,[DATE_OF_BIRTH]                         
      ,INSERTED_DATE    ,DTYPE ,IS_PRIMARY ,UPPER(REPLACE(REPLACE([ADDRESS] ,'~',' '),'null' ,'')) [ADDRESS]                       
     FROM (                                      
     SELECT *                                      
     FROM                                      
     (                                      
     SELECT  GSTIN,                       
      UPPER(CONCAT([FIRST_NAME],' ',[MIDDLE_NAME],' ',[LAST_NAME])) [OWNER_NAME]                                      
      --,UPPER(CONCAT([FATHER_FIRST_NAME],' ',[FATHER_MIDDLE_NAME],' ',[FATHER_LAST_NAME] ))  OWNER_FATHER_NAME                                      
                                           
      ,UPPER([PAN_NO])  [PAN_NO]                                      
   ,[MOBILE_NO]                                      
   ,LOWER([EMAIL])  [EMAIL_ID]                                      
   ,UPPER([DESIGNATION]) [DESIGNATION]                                      
   ,CONVERT(DATE, [DOB],103)  [DATE_OF_BIRTH]                                
   --,ROW_NUMBER() OVER(PARTITION BY A.GSTIN ORDER BY A.GSTIN) AS RNK                                    
      ,[INSERTED_DATE] [INSERT_DATE]                        
 ,DTYPE                      
   ,IS_PRIMARY                         
                       
  ,ADDRESS, VALUE  , ROW_NUMBER() OVER (PARTITION BY GSTIN ,ADDRESS,FIRST_NAME , MIDDLE_NAME , LAST_NAME ,DESIGNATION ,DOB, DTYPE ,IS_PRIMARY  ORDER BY GSTIN) AS ROWID ,INSERTED_DATE                                     
     FROM  [TEST_MPCTD].[DBO].[TBL_REG_AUTH_OWNER_DETAIL]                                     
         CROSS APPLY STRING_SPLIT(ADDRESS, '~')                        
       ---@DATE                      
   WHERE CONVERT(DATE ,INSERTED_DATE,103) >= @INSERT_DATE                                   
     ) SRC                      
     PIVOT                                      
     (MAX(VALUE) FOR ROWID IN ([1],[2] , [3] , [4],[5], [6], [7], [8], [9],[10],[11], [12], [13], [14],[15]))P                                      
     ) F                                      
)                        
,FINAL_AUTH_SIGN AS                      
(                      
SELECT A.[GSTIN]                                      
    ,A.PINCODE [PIN_CODE]                  
    ,[OWNER_COUNT]                                
    ,[OWNER_NAME]                                 
    ,[PAN_NO]                                     
    ,[MOBILE_NO]                                  
    ,[EMAIL_ID]                                   
    ,[DESIGNATION]                                
    ,[DATE_OF_BIRTH]                      
 ,A.INSERTED_DATE [INSERT_DATE]                       
 ,DTYPE                      
 ,IS_PRIMARY                       
 ,ADDRESS                        
FROM AUTH_SIGN A                      
JOIN  (                                      
   SELECT GSTIN , COUNT(UPPER(CONCAT([FIRST_NAME],' ',[MIDDLE_NAME],' ',[LAST_NAME]))) [OWNER_COUNT]                                      
   FROM [TEST_MPCTD].[dbo].[TBL_REG_AUTH_OWNER_DETAIL]  GROUP BY GSTIN )  O                               
            ON A.GSTIN = O.GSTIN                         
      )                      
                         
                      
   INSERT INTO [STAGING_GST_IMPL_API].[dbo].[AUTH_SIGNATORY_DETAILS_API]                                      
(       [GSTIN]                                      
      ,[PIN_CODE]                                      
      ,[OWNER_COUNT]                                      
      ,[OWNER_NAME]                                      
      ,[PAN_NO]                                      
      ,[MOBILE_NO]                                      
      ,[EMAIL_ID]                                      
      ,[DESIGNATION]                                      
      ,[DATE_OF_BIRTH]                                      
      ,[INSERT_DATE]                       
   ,[DTYPE]                      
   , [IS_PRIMARY]                        
   ,ADDRESS                                      
)                                
                              
   SELECT   [GSTIN]                                      
    ,[PIN_CODE]                                   
    ,[OWNER_COUNT]                                
    ,[OWNER_NAME]                                 
    ,[PAN_NO]                                     
    ,[MOBILE_NO]                                  
    ,[EMAIL_ID]                                   
    ,[DESIGNATION]                                
    ,[DATE_OF_BIRTH] ,[INSERT_DATE]                       
 ,DTYPE                      
 ,IS_PRIMARY                       
 ,ADDRESS                        
 FROM FINAL_AUTH_SIGN A                             
   WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[AUTH_SIGNATORY_DETAILS_API]   B                        
  WHERE  A.[GSTIN] = B.[GSTIN]   COLLATE LATIN1_GENERAL_CI_AI                       
  AND A.ADDRESS = B.ADDRESS COLLATE LATIN1_GENERAL_CI_AI                       
  AND A.[OWNER_NAME] = B.[OWNER_NAME]  COLLATE LATIN1_GENERAL_CI_AI                       
  AND A.[PAN_NO] = B.[PAN_NO]       COLLATE LATIN1_GENERAL_CI_AI                       
  AND A.DESIGNATION = B.DESIGNATION  COLLATE LATIN1_GENERAL_CI_AI                         
  AND A.DTYPE = B.DTYPE COLLATE LATIN1_GENERAL_CI_AI                       
  AND A.IS_PRIMARY  = B.IS_PRIMARY   COLLATE LATIN1_GENERAL_CI_AI )                      
                      
AND CONVERT(DATE ,[INSERT_DATE],103) >=   @INSERT_DATE                       
               
                           
   --17543     585                               
                                    
   -- select * from [STAGING_GST_IMPL_API].[dbo].[AUTH_SIGNATORY_DETAILS_API] where CONVERT(DATE ,[INSERT_DATE],103) =  '2023-10-12'                                  
   --------------------------------                                      
                                      
--   SELECT * FROM DEALER_REGISTARTION_API                                    
-- ALTER TABLE DEALER_REGISTARTION_API  ADD  PAN_NO  VARCHAR(50)                                    
--ALTER TABLE DEALER_REGISTARTION_API  ALTER COLUMN  ADDRESS  VARCHAR(MAX)                                    
--ALTER TABLE DEALER_REGISTARTION_API  ADD  [RISK_PROFILE]  VARCHAR(50)                                        
--ALTER TABLE DEALER_REGISTARTION_API  ADD  [CIRCLE_CODE]  VARCHAR(50)                                      
--ALTER TABLE DEALER_REGISTARTION_API  ADD  CENTER_JURISDICTION_CODE VARCHAR(50)                                      
---truncate table [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API                                      
                                      
INSERT INTO [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API                                      
(                                      
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
   ,CENTER_JURISDICTION_CODE                          
   ,RISK_PROFILE                                     
   , ADDRESS                                    
   , PAN_NO                                     
   )                                      
SELECT [GSTIN]                                      
,upper([TRADE_NAME]) [TRADE_NAME]                                      
,UPPER([LEGAL_NAME]) [LEGAL_NAME]                                      
,[REG_TYPE_CD]  [REG_TYPE]                                      
,UPPER([AUTHORISED_STATUS])   [STATUS_TYPE]                                     
--,UPPER([REG_CATEGORY])    [REG_CATEGORY]                                      
,CONVERT(DATE, [REG_DATE] , 103) [DT_REG]                                      
,CONVERT(DATE , CANCEL_DATE , 103)  [DT_DEREG]                                      
, CASE WHEN UPPER([APPROVED_AUTHORITY])= 'STATE' then 'S' when UPPER([APPROVED_AUTHORITY])= 'CENTER' THEN 'C' END  JURISDICTION                                          
,NULL CIRCLE                              
,NULL DIVISION                      
      ,[MOBILE_NO]                                      
    ,[EMAIL]                                      
    ,NULL [NTBZ]                                      
      , NULL [NTBZ_DESCRIPTION]                                      
      ,NULL [NTBZ_PRIORITY]                                      
      ,NULL [NTBZ_ALL]                                    
      ,NULL [NTBZ_DESCRIPTION_ALL]                                     
   ,[COBZ] [COBZ_CD]                                      
   ,[INSERTED_DATE]  INSERT_DATE                                      
   ,[STATE_JURISDICTION]   [CIRCLE_CODE]                                      
      ,CENTER_JURISDICTION    CENTER_JURISDICTION_CODE                                     
   , RISK_PROFILE                                     
   ,UPPER(REPLACE(REPLACE([ADDRESS] ,'~',' '),'null' ,'')) [ADDRESS]                                    
   ,PAN_NO                                       
   FROM [TEST_MPCTD].[dbo].[TBL_REG_REGULAR_TAX_PAYER] A                        
   WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API    B                        
     WHERE  A.[GSTIN] = B.[GSTIN]   COLLATE Latin1_General_CI_AI  )                          
   AND CONVERT(DATE ,INSERTED_DATE,103) >= @INSERT_DATE;                          
                             
                          
   ----------------------------------------------------------------------------------------------------------                          
                          
                                        
INSERT INTO [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API                                      
(                                      
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
   ,CENTER_JURISDICTION_CODE                                    
   ,RISK_PROFILE                                     
   , ADDRESS                                    
   , PAN_NO                           
                             
   )                                      
SELECT [GSTIN]                                      
,upper([TRADE_NAME]) [TRADE_NAME]                                      
,UPPER([LEGAL_NAME_OF_TAX_DEDUCTOR]) [LEGAL_NAME]                                      
,[APPLICATION_CODE]  [REG_TYPE]                                      
,UPPER([APPLICATION_STATUS])   [STATUS_TYPE]                                      
--,UPPER([REG_CATEGORY])    [REG_CATEGORY]                                      
,CONVERT(DATE, [REG_DATE] , 103) [DT_REG]                                      
,CONVERT(DATE , CANCEL_DATE , 103)  [DT_DEREG]                                      
, case when UPPER([APPROVED_AUTHORITY])= 'STATE' then 'S'                             
             when UPPER([APPROVED_AUTHORITY])= 'CENTER' THEN 'C' END  JURISDICTION                                      
,NULL CIRCLE                                      
,NULL DIVISION                                      
      ,[MOBILE_NO]                                      
    ,[EMAIL]                                      
    ,NULL [NTBZ]                                      
      , NULL [NTBZ_DESCRIPTION]                                      
      ,NULL [NTBZ_PRIORITY]                     
      ,NULL [NTBZ_ALL]                                      
      ,NULL [NTBZ_DESCRIPTION_ALL]                                     
   ,[COBZ] [COBZ_CD]                                      
   ,[INSERTED_DATE]  INSERT_DATE                                      
   ,[STATE_JURISDICTION]   [CIRCLE_CODE]                                      
      ,CENTER_JURISDICTION    CENTER_JURISDICTION_CODE                                     
   , RISK_PROFILE                                     
   ,UPPER(REPLACE(REPLACE([PPB_ADDRESS] ,'~',' '),'null' ,'')) [ADDRESS]                                    
   ,PAN_NO                            
                           
   FROM [TEST_MPCTD].[dbo].[TBL_REG_TDS_TCS]  A                        
     WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API    B                        
     WHERE  A.[GSTIN] = B.[GSTIN]   COLLATE Latin1_General_CI_AI  )                          
   AND CONVERT(DATE ,INSERTED_DATE,103) >= @INSERT_DATE                          
                           
   ;                          
                          
                          
                                      
                                  
                                  
   ---------------------------------------------------------------------------------------------------------------                                      
  --- CREATE DEALER_NTBZ_DEATILS_TABLES                                       
                                      
 --  SELECT * FROM ZONE_DIVISION_CIRCLE                                      
--create table DEALER_NTBZ_DEATAILS                                      
--(                                       
--NTBZ VARCHAR(20) ,                                      
--NTBZ_DESCRIPTION VARCHAR(50) ,                                      
--NTBZ_PRIORITY INT ,                                      
--NTBZ_DESCRIPTION_PRIORITY VARCHAR(50)                                      
--)                        
                                      
                                      
--INSERT INTO DEALER_NTBZ_DEATAILS                                      
--select distinct NTBZ                                      
--,NTBZ_DESCRIPTION                                      
--,cast (NTBZ_PRIORITY as int) NTBZ_PRIORITY                                       
--, concat(NTBZ_Description,' (',NTBZ_PRIORITY,')') NTBZ_Description_priority                                      
--from GST_IMPL_NEW..DEALER_MASTER                                       
--order by cast (NTBZ_PRIORITY as int)                                      
                                      
 --SELECT * FROM DEALER_NTBZ_DEATAILS                                      
                                       
   ---------------------------------------------------------------------------                                  
BEGIN                                  
                                  
 ; WITH RESULT AS                                  
(                                  
SELECT gstin, NTBZ, value as NTBZ_SPLIT , ROW_NUMBER() OVER (PARTITION BY GSTIN ORDER BY GSTIN) AS RoWID                                  
FROM [TEST_MPCTD].[dbo].[TBL_REG_REGULAR_TAX_PAYER]                                  
    CROSS APPLY STRING_SPLIT(NTBZ, '~')                                  
 )                                  
, NTBZ_FINAL AS                                  
(                
 select B.GSTIN , B.NTBZ_SPLIT, B.RoWID ,NTBZ_DESCRIPTION ,NTBZ_PRIORITY ,NTBZ_ALL,NTBZ_DESCRIPTION_PRIORITY                                  
 from [dbo].[DEALER_NTBZ_DEATAILS] a                                  
 join RESULT b                                  
 on a.NTBZ= b.NTBZ_SPLIT  COLLATE SQL_Latin1_General_CP1_CI_AS                                   
 )                                  
 , NTBZ_AGG AS            ---17543                                  
 (                                  
 SELECT GSTIN ,                                  
  STRING_AGG(NTBZ_ALL ,',') NTBZ_ALL ,                                   
  STRING_AGG(NTBZ_DESCRIPTION_PRIORITY ,',') NTBZ_DESCRIPTION_ALL                                  
 FROM NTBZ_FINAL                                  
 GROUP BY GSTIN                                    
 )                                  
 ,NTBZ_PRIO AS        ---17543       
 (                                  
 SELECT GSTIN ,NTBZ_SPLIT NTBZ , NTBZ_DESCRIPTION ,NTBZ_PRIORITY                                  
FROM NTBZ_FINAL                                   
 WHERE RoWID =1                                   
 )                                  
 , NTBZ_MAIN AS                                  
 (                                  
 SELECT A.GSTIN , NTBZ , NTBZ_DESCRIPTION ,NTBZ_PRIORITY ,NTBZ_ALL ,NTBZ_DESCRIPTION_ALL                                  
 FROM NTBZ_AGG  A                                  
 JOIN NTBZ_PRIO  P                                  
 ON A.GSTIN = P.GSTIN                                    
 )                                  
 UPDATE [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API                                  
 SET [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.NTBZ = A.NTBZ                                   
 , [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.NTBZ_DESCRIPTION  = A.NTBZ_DESCRIPTION                                     
 , [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.NTBZ_PRIORITY =  A.NTBZ_PRIORITY                                    
 , [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.NTBZ_ALL = A.NTBZ_ALL                                    
 , [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.NTBZ_DESCRIPTION_ALL = A.NTBZ_DESCRIPTION_ALL                                   
 FROM NTBZ_MAIN A                                  
 WHERE [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API.GSTIN = A.GSTIN  COLLATE SQL_Latin1_General_CP1_CI_AS                                  
                                  
END ;                                  
                                   
 --SELECT * FROM [STAGING_GST_IMPL_API].[dbo].DEALER_REGISTARTION_API                                  
 --------------------------------------------------------------------------------------------------------------                                      
                                      
 ---GSTIN_ADDRESS_API                                      
                                      
                                      
INSERT INTO [STAGING_GST_IMPL_API].[dbo].[GSTN_ADDRESS_API]                                      
SELECT F.GSTIN , UPPER(REPLACE(REPLACE([ADDRESS] ,'~',' '),'null' ,'')) [ADDRESS] ,                                      
 UPPER(REPLACE(REPLACE(F.[8] ,'~',' '),'null' ,''))  PINCODE , F.[9]  [LATITUDE]   ,F.[10] LONGITUDE                            
 ,INSERTED_DATE                          
FROM (                                      
select *                                      
from                                      
(                                      
SELECT gstin, address, value  , ROW_NUMBER() OVER (PARTITION BY GSTIN ORDER BY GSTIN) AS RoWID ,INSERTED_DATE                             
FROM  [TEST_MPCTD]..[TBL_REG_REGULAR_TAX_PAYER]                                      
    CROSS APPLY STRING_SPLIT(address, '~')                           
                        
WHERE CONVERT(DATE ,INSERTED_DATE,103) >= @INSERT_DATE                                   
) src                                      
pivot                                      
(max(value) for rowid in ([1],[2] , [3] , [4],[5], [6], [7], [8], [9],[10],[11], [12], [13], [14],[15]))p                                      
) F                                      
 WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[GSTN_ADDRESS_API]     B                        
     WHERE  F.[GSTIN] = B.[GSTIN]   COLLATE Latin1_General_CI_AI  )                            
                             
                                        
--SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[GSTN_ADDRESS_API]                                      
                                      
end ;                                      
/*                                      
  CREATE TABLE REG_GOODS_SERVICE_DETAIL_API                        
  ( GSTIN VARCHAR(50) ,                                      
    HSN_SAC_CODE VARCHAR(50),                                      
 DTYPE VARCHAR(50) ,                                      
 HSN_SAC_DESC VARCHAR(MAX)                                      
)                                      
*/                                      
----------------SECTOR_OF_BUSINESS---------------------------                                
BEGIN                                
                                
 INSERT INTO [STAGING_GST_IMPL_API].[DBO].[DEALER_SECTOR_BUSINESS_API]                                 
 SELECT                                 
 DISTINCT GSTIN ,                                 
 NULL SECTOR_OF_BUSINESS ,                                
 INSERT_DATE                                
 FROM [STAGING_GST_IMPL_API].[DBO].[DEALER_REGISTARTION_API]   A                         
 WHERE NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_SECTOR_BUSINESS_API]     B                        
     WHERE  A.[GSTIN] = B.[GSTIN]   COLLATE Latin1_General_CI_AI  )                               
 AND CONVERT(DATE ,INSERT_DATE,103) >= @INSERT_DATE                           
                         
                          
 END ;                                
 -------------------------------------------------------------------------                                
                              
                               
   INSERT INTO REG_GOODS_SERVICE_DETAIL_API                                      
    SELECT GSTIN  ,                                      
    HSN_SAC_CD  HSN_SAC_CODE,                                      
 DTYPE  ,                                      
 UPPER(HSN_SAC_DESC) HSN_SAC_DESC , [INSERTED_DATE]                                    
  FROM [TEST_MPCTD].[dbo].[TBL_REG_GOODS_SERVICE_DETAIL]                           
  WHERE CONVERT(DATE ,[INSERTED_DATE],103) >=  @INSERT_DATE                                     
                           
   --SELECT COUNT(*) FROM REG_GOODS_SERVICE_DETAIL_API    104198                        
                          
                                      
                                      
begin                                      
                             
                          
  IF OBJECT_ID('[STAGING_GST_IMPL_API].[DBO].[DEALER_HSN_MASTER_API]', 'U') IS NOT NULL                            
BEGIN                            
    -- Truncate the table if it exists                            
    TRUNCATE TABLE [STAGING_GST_IMPL_API].[DBO].[DEALER_HSN_MASTER_API];                            
END                          
                          
                                   
;WITH COUNT_FREQUENCY AS                                        
(                                        
select GSTIN , LEFT(HSN_SAC_CODE ,2) HSN_CODE                                          
, COUNT(1) OVER( PARTITION BY GSTIN ,LEFT(HSN_SAC_CODE ,2) ) FREQUENCY                                        
, COUNT(1) OVER( PARTITION BY GSTIN ) CHAPTER_COUNT                                        
from [STAGING_GST_IMPL_API].[dbo].[REG_GOODS_SERVICE_DETAIL_API]                                        
 --WHERE GSTIN IN ('23AABAP7624D1ZW' , '23AAJPC7405L1ZU' )                                        
 )                                        
 ,HSN_MAX_FREQUENCY AS                                        
 (SELECT * , MAX(FREQUENCY) OVER(PARTITION BY GSTIN ) AS MAX_CHAPTER_FREQUENCY                        
                                        
 FROM COUNT_FREQUENCY                                        
 )                                        
, RANK_FREQ AS                                        
 (                                        
 SELECT a.*,                                         
 DENSE_RANK() OVER(PARTITION BY a.GSTIN ORDER BY a.GSTIN , frequency desc, MAX_CHAPTER_FREQUENCY desc , HSN_CODE ) HSN_RANK                                        
 FROM HSN_MAX_FREQUENCY a                                        
 )                                        
 ,FINAL AS                                        
 (                                        
  SELECT F.* ,                                         
 CASE WHEN HSN_RANK = 1 THEN 'Y' ELSE 'N' END AS IS_PRINCIPAN_CHAPTER                                        
 FROM RANK_FREQ F                                         
)                                        
 INSERT INTO [STAGING_GST_IMPL_API].[dbo].[DEALER_HSN_MASTER_API] (GSTIN , HSN ,FREQUENCY ,CHAPTER_COUNT ,MAX_CHAPTER_FREQUENCY ,IS_PRINCIPAN_CHAPTER )                                        
                                         
 SELECT GSTIN  , HSN_CODE ,FREQUENCY ,CHAPTER_COUNT ,MAX_CHAPTER_FREQUENCY ,IS_PRINCIPAN_CHAPTER                                        
 FROM FINAL                                          
                                        
                                         
  END;                 
                                      
                                   
   -------------------------------------------------------------                                
   BEGIN                                
                                
   UPDATE [STAGING_GST_IMPL_API].[dbo].[DEALER_SECTOR_BUSINESS_API]                                
  SET SECTOR_OF_BUSINESS = A.HSN                                
 FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_HSN_MASTER_API] A                                
  WHERE [STAGING_GST_IMPL_API].[dbo].[DEALER_SECTOR_BUSINESS_API].GSTIN = A. GSTIN                                
 AND IS_PRINCIPAN_CHAPTER = 'Y'                                  
                                
  END ;                                
  ---------------------------------------------------------                                
   update [STAGING_GST_IMPL_API].[dbo].[DEALER_REGISTARTION_API]                    
   set [REG_TYPE] =  a.[REG_TYPE_CD]                      
       ,[STATUS_TYPE] = UPPER(a.[AUTHORISED_STATUS])                      
    ,  [DT_REG]   =   CONVERT(DATE, a.[REG_DATE] , 103)                     
    , [DT_DEREG]    =  CONVERT(DATE , a.CANCEL_DATE , 103)                   
    ,[CIRCLE_CODE]  = a.[STATE_JURISDICTION]                      
   from TEST_MPCTD.[dbo].[TBL_REG_REGULAR_TAX_PAYER] a                     
   where [STAGING_GST_IMPL_API].[dbo].[DEALER_REGISTARTION_API].gstin = a.gstin collate SQL_Latin1_General_CP1_CI_AS ;                   
      
                     
   SET ansi_warnings OFF     
   --------------------------------------------------------------------------------------------------                       
       IF OBJECT_ID('[STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]', 'U') IS NOT NULL                      
BEGIN                      
    -- Truncate the table if it exists                      
    TRUNCATE TABLE [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API];                      
END              
            
                     
  begin                                
                                
                                 
;WITH REG_DEALER AS                                
(                                
SELECT T.*                                 
FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_REGISTARTION_API] T           ---17543   ----796279                                
 )                                                 ---785000                                
, RESULT as                                
(                                
SELECT  A.*  ,  C.ZONE , C.[ZONE_CODE]                    
    ,[DIVISION_CODE]                          
    ,C.[CIRCLE] [CIRCLE_ZONE] , C.[DIVISION] [DIVISION_ZONE]                              
FROM REG_DEALER A                                 
LEFT JOIN  [STAGING_GST_IMPL_API].[dbo].[ZONE_DIVISION_CIRCLE] C                                
ON  A.CIRCLE_CODE= C.CIRCLE_CODE                               
-- LEFT join [STAGING_GST_IMPL_API].[dbo].[AUTH_SIGNATORY_DETAILS_API] B                                
--on A.GSTIN= B.GSTIN                                  ---796279                                
 )                                                                              
, DEALER_FINAL AS                                
(                                
Select DISTINCT r.[GSTIN]                                
      ,upper(r.[TRADE_NAME]) [TRADE_NAME]                                
      ,upper(r.[LEGAL_NAME]) [LEGAL_NAME]                       
   ,r.[REG_TYPE]                                
      ,r.[STATUS_TYPE]                                
      ,r.[DT_REG]                                
      ,r.[DT_DEREG]                                
      ,r.[JURISDICTION]                                
      ,r.[CIRCLE_ZONE] [CIRCLE]                                
      ,r.[DIVISION_ZONE] [DIVISION]                                
      ,r.[MOBILE_NO]                                
      ,r.[EMAIL]                                
      ,r.[NTBZ]                                
      ,r.[NTBZ_DESCRIPTION]                                
      ,r.[NTBZ_PRIORITY]                          
      ,r.[NTBZ_ALL]                                
      ,r.[NTBZ_DESCRIPTION_ALL]                                
      ,r.[COBZ_CD]                               
   , r.PAN_NO                                
      , NULL SECTOR_OF_BUSINESS                               
      ,r.ZONE                               
      ,r.[ADDRESS]                               
      , CENTER_JURISDICTION_CODE as CENTER_CODE                               
      ,null BANK_ACCOUNT_NO                                
      ,r.[ZONE_CODE]                                
      ,r.[DIVISION_CODE]                                
      ,r.[CIRCLE_CODE]                                
   , r.Risk_Profile                   
   ,r.INSERT_DATE                 
from result r                                
                          ---796279                                
                                           ---785000                                
)                                
--select *                                
--FROM DEALER_FINAL                                 
--  WHERE CONVERT(DATE ,INSERT_DATE,103) <='2023-10-13' @INSERT_DATE                     
                                
INSERT into [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]   --(796279 rows affected)     (785000 rows affected)                                
select *                                
FROM DEALER_FINAL                   
             
-- NOT EXISTS (SELECT * FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]     B                  
--     WHERE  A.[GSTIN] = B.[GSTIN]   COLLATE Latin1_General_CI_AI  )                    
--AND              
                            
                                  
    --   select count(1)  from     [DEALER_MASTER_API]     --  40624            
                              
 -- ALTER TABLE [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER] ADD INSERT_DATE   date    31943                                               
  --select count(*) from  [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]   WHERE CONVERT(DATE ,INSERT_DATE,103) ='2023-10-12'                            
                 
   --truncate table [DEALER_MASTER_API]              
                 
                                
  /*   Validation Dealer Master                                                                    
                                 
SELECT distinct gstin  FROM  DEALER_FINAL      ---7850000                                
GROUP BY [STATUS_TYPE]      
                                
 A 518813                                
C 10346                                
CBT 118471                                
E 859                                
I 3688                                
P 533                                
SC 116894                                
SP 15394      U 2                                
SELECT [STATUS_TYPE] , COUNT(*) FROM  DEALER_FINAL                                
GROUP BY [STATUS_TYPE]                                
                                
                                
SELECT [STATUS_TYPE] , COUNT(*) FROM [dbo].[DEALER_REGISTARTION_NEW]                                
GROUP BY [STATUS_TYPE]                                
*/                                
                                
--INSERT into [GST_IMPL_NEW].[dbo].[DEALER_MASTER_API]   --(692328 rows affected)                                
                                
                                
--select REG_TYPE , COUNT(*)                                
--FROM [dbo].[DEALER_MASTER_API]                                  
--GROUP BY REG_TYPE                                 
                                
--APLTC 680                                
--APLTD 20374                                
--CA 1548                                
--CO 81010                                
--ID 327                                
--NT 692274                                
--TP 66                                
                                
--APLTC 660                                
--APLTD 19424                                
--CA 1504                                
--CO 80549                                
--ID 326                                
--NT 682472                                
--TP 65                                
                                
--SELECT (660                                
--+19424                                
--+1504                                
--+80549                                
--+326                                
--+682472                                
--+65)   ==-- 785000                                
-----710731                                
                                
 -- select * from [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]                              
                              
----UPDATE SECTOR_OF_BUSINESS OR  IS_PRINCIPAN_CHAPTER                                
                                
UPDATE [STAGING_GST_IMPL_API].[dbo].DEALER_MASTER_API           
SET SECTOR_OF_BUSINESS=A.HSN                                
FROM [STAGING_GST_IMPL_API].[dbo].[DEALER_HSN_MASTER_API] A                                
WHERE A.GSTIN = [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API].GSTIN             ---(763754 rows affected)                                
AND A.IS_PRINCIPAN_CHAPTER= 'Y'                               
                              
 ---and  [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API].SECTOR_OF_BUSINESS is null ;                  ----((763502 rows affected))                                
                                
                                  
                          
--UPDATE DEALER_MASTER_API                                
--set CENTER_CODE =CENTER_JURISDICTION                                
--FROM [TEST_MPCTD].[dbo].[TBL_REG_REGULAR_TAX_PAYER] A                                
--where DEALER_MASTER_API.GSTIN=A.GSTIN  collate SQL_Latin1_General_CP1_CI_AS and DEALER_MASTER_API.CENTER_CODE is null   ---5360 rows affected)                                
                                
                                
                               
                                
---BANK ACCOUNT DEATILAS UPDATE                                 
                                
;WITH RESULT AS          -----(688893 rows affected)    (686927 rows affected)                                
(                                
SELECT A.GSTIN , A.ACCOUNT_NO ,A.RN                                
FROM (                                
SELECT  [GSTIN]                                
      ,[STATE_CODE]                                
      ,[BANK_NAME]                              
      ,[IFSC_CODE]                                
      ,[BANK_ADDRESS]                                
      ,[ACCOUNT_NO] , ROW_NUMBER() OVER(PARTITION BY GSTIN ORDER BY ACCOUNT_NO) AS RN                       
  FROM [GST_IMPL_NEW].[dbo].[DEALER_BANK_DETAILS] ) A                                
                                
  WHERE A.RN= 1 )                                
UPDATE [STAGING_GST_IMPL_API].[dbo].DEALER_MASTER_API                                
SET BANK_ACCOUNT_NO = A.ACCOUNT_NO                                
FROM RESULT  A                                
WHERE A.GSTIN = [STAGING_GST_IMPL_API].[dbo].DEALER_MASTER_API.GSTIN                                
                                
-------------------------------------------------------------------------------------                                
-- select * from [dbo].[DEALER_MASTER]                      
--WHERE Jurisdiction IN ('STATE' , 'CENTER')        --24769                      
                      
--UPDATE [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API]                      
--set Jurisdiction =  case when Jurisdiction= 'STATE' then 'S'                       
--             when Jurisdiction= 'CENTER' THEN 'C' END                      
--WHERE Jurisdiction IN ('STATE' , 'CENTER')                           
                                
                            
-----------MERGING LOGIC OF DEALER MASTER ------------------------------                            
                            
Merge GST_IMPL_NEW_TEST.[dbo].[DEALER_MASTER]   as Target                            
Using [STAGING_GST_IMPL_API].[dbo].[DEALER_MASTER_API] as Source                            
ON (Target.GSTIN= SOURCE.GSTIN )                            
WHEN NOT MATCHED BY TARGET                           
THEN                             
 INSERT ([GSTIN]                            
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
           ,[PAN_NO]                            
           ,[SECTOR_OF_BUSINESS]                            
           ,[ZONE]                            
           ,[ADDRESS]                            
           ,[CENTER_CODE]                            
           ,[BANK_ACCOUNT_NO]                            
           ,[ZONE_CODE]                            
           ,[DIVISION_CODE]                            
           ,[CIRCLE_CODE])                            
 VALUES (SOURCE.[GSTIN]                            
           ,SOURCE.[TRADE_NAME]                            
           ,SOURCE.[LEGAL_NAME]                            
           ,SOURCE.[REG_TYPE]                            
           ,SOURCE.[STATUS_TYPE]                            
           ,SOURCE.[DT_REG]                            
           ,SOURCE.[DT_DEREG]                            
           ,SOURCE.[JURISDICTION]                            
           ,SOURCE.[CIRCLE]                            
           ,SOURCE.[DIVISION]                            
           ,SOURCE.[MOBILE_NO]                            
           ,SOURCE.[EMAIL]                            
           ,SOURCE.[NTBZ]                            
           ,SOURCE.[NTBZ_DESCRIPTION]                            
           ,SOURCE.[NTBZ_PRIORITY]                     ,SOURCE.[NTBZ_ALL]                            
           ,SOURCE.[NTBZ_DESCRIPTION_ALL]                            
           ,SOURCE.[COBZ_CD]                            
           ,SOURCE.[PAN_NO]                            
           ,SOURCE.[SECTOR_OF_BUSINESS]                            
         ,SOURCE.[ZONE]                            
           ,SOURCE.[ADDRESS]                            
           ,SOURCE.[CENTER_CODE]                            
           ,SOURCE.[BANK_ACCOUNT_NO]                            
  ,SOURCE.[ZONE_CODE]                            
           ,SOURCE.[DIVISION_CODE]                            
           ,SOURCE.[CIRCLE_CODE])                            
WHEN MATCHED AND                             
    (TARGET.[TRADE_NAME]            <> SOURCE.[TRADE_NAME]                            
              OR TARGET.[LEGAL_NAME]            <> SOURCE.[LEGAL_NAME]                            
     OR TARGET.[REG_TYPE]    <> SOURCE.[REG_TYPE]                            
     OR TARGET.[STATUS_TYPE]   <> SOURCE.[STATUS_TYPE]                            
     OR TARGET.[DT_REG]    <> SOURCE.[DT_REG]                            
     OR TARGET.[DT_DEREG]    <> SOURCE.[DT_DEREG]                            
     OR TARGET.[JURISDICTION]   <> SOURCE.[JURISDICTION]                            
     OR TARGET.[CIRCLE]    <> SOURCE.[CIRCLE]                            
     OR TARGET.[DIVISION]    <> SOURCE.[DIVISION]                            
     OR TARGET.[MOBILE_NO]    <> SOURCE.[MOBILE_NO]                            
     OR TARGET.[EMAIL]     <> SOURCE.[EMAIL]                            
     OR TARGET.[NTBZ]     <> SOURCE.[NTBZ]                            
     OR TARGET.[NTBZ_DESCRIPTION]  <> SOURCE.[NTBZ_DESCRIPTION]                            
     OR TARGET.[NTBZ_PRIORITY]   <> SOURCE.[NTBZ_PRIORITY]                            
     OR TARGET.[NTBZ_ALL]    <> SOURCE.[NTBZ_ALL]                            
     OR TARGET.[NTBZ_DESCRIPTION_ALL] <> SOURCE.[NTBZ_DESCRIPTION_ALL]                            
     OR TARGET.[COBZ_CD]    <> SOURCE.[COBZ_CD]                            
     OR TARGET.[PAN_NO]    <> SOURCE.[PAN_NO]                            
     OR TARGET.[SECTOR_OF_BUSINESS] <> SOURCE.[SECTOR_OF_BUSINESS]                            
     OR TARGET.[ZONE]     <> SOURCE.[ZONE]                            
     OR TARGET.[ADDRESS]    <> SOURCE.[ADDRESS]                            
     OR TARGET.[CENTER_CODE]   <> SOURCE.[CENTER_CODE]                            
     OR TARGET.[BANK_ACCOUNT_NO]  <> SOURCE.[BANK_ACCOUNT_NO]                            
     OR TARGET.[ZONE_CODE]    <> SOURCE.[ZONE_CODE]                            
     OR TARGET.[DIVISION_CODE]   <> SOURCE.[DIVISION_CODE]                            
     OR TARGET.[CIRCLE_CODE]   <> SOURCE.[CIRCLE_CODE] )                            
                            
THEN UPDATE SET                             
              TARGET.[TRADE_NAME]            = SOURCE.[TRADE_NAME]                            
              ,  TARGET.[LEGAL_NAME]            = SOURCE.[LEGAL_NAME]           
     ,  TARGET.[REG_TYPE]    = SOURCE.[REG_TYPE]                            
     ,  TARGET.[STATUS_TYPE]   = SOURCE.[STATUS_TYPE]                            
     ,  TARGET.[DT_REG]    = SOURCE.[DT_REG]                            
     ,  TARGET.[DT_DEREG]    = SOURCE.[DT_DEREG]                            
     ,  TARGET.[JURISDICTION]   = SOURCE.[JURISDICTION]                            
     ,  TARGET.[CIRCLE]    = SOURCE.[CIRCLE]                            
     ,  TARGET.[DIVISION]    = SOURCE.[DIVISION]                            
     ,  TARGET.[MOBILE_NO]    = SOURCE.[MOBILE_NO]                            
     ,  TARGET.[EMAIL]     = SOURCE.[EMAIL]                            
     ,  TARGET.[NTBZ]     = SOURCE.[NTBZ]                            
     ,  TARGET.[NTBZ_DESCRIPTION]  = SOURCE.[NTBZ_DESCRIPTION]                            
     ,  TARGET.[NTBZ_PRIORITY]   = SOURCE.[NTBZ_PRIORITY]                            
     ,  TARGET.[NTBZ_ALL]    = SOURCE.[NTBZ_ALL]                            
     ,  TARGET.[NTBZ_DESCRIPTION_ALL] = SOURCE.[NTBZ_DESCRIPTION_ALL]                            
     ,  TARGET.[COBZ_CD]    = SOURCE.[COBZ_CD]                            
     ,  TARGET.[PAN_NO]    = SOURCE.[PAN_NO]                            
     ,  TARGET.[SECTOR_OF_BUSINESS] = SOURCE.[SECTOR_OF_BUSINESS]                            
     ,  TARGET.[ZONE]     = SOURCE.[ZONE]                            
     ,  TARGET.[ADDRESS]    = SOURCE.[ADDRESS]                            
     ,  TARGET.[CENTER_CODE]   = SOURCE.[CENTER_CODE]                            
     ,  TARGET.[BANK_ACCOUNT_NO]  = SOURCE.[BANK_ACCOUNT_NO]                            
     ,  TARGET.[ZONE_CODE]    = SOURCE.[ZONE_CODE]                            
     ,  TARGET.[DIVISION_CODE]   = SOURCE.[DIVISION_CODE]                            
     ,  TARGET.[CIRCLE_CODE]   = SOURCE.[CIRCLE_CODE]   ;                            
                            
                             
                                 
                            
END;                   
                  
                  
