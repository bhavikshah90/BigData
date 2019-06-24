crime_data = LOAD '/home/bhavik/Desktop/bigdata/Crimes_cleaned.csv' USING PigStorage(',') as (ID:chararray,Case_Number:chararray,Date:chararray,Block:chararray,IUCR:chararray,Primary_Type:chararray,Description:chararray,Location_Description:chararray,Arrest:Boolean,Domestic:Boolean,Beat:chararray,District:chararray,Ward:chararray,Community_Area:chararray,FBI_Code:chararray,Year:chararray);
rh = filter crime_data by $2 != 'Date';
gr = GROUP rh BY (FBI_Code);
gen = FOREACH gr GENERATE group AS (Fbi), COUNT(rh.FBI_Code);
orders = ORDER gen BY $1 DESC;
STORE orders into 'fbi' using PigStorage(',');
