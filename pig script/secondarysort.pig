crime_data = LOAD '/home/bhavik/Desktop/bigdata/Crimes_cleaned.csv' USING PigStorage(',') as (ID:chararray,Case_Number:chararray,Date:chararray,Block:chararray,IUCR:chararray,Primary_Type:chararray,Description:chararray,Location_Description:chararray,Arrest:Boolean,Domestic:Boolean,Beat:chararray,District:chararray,Ward:chararray,Community_Area:chararray,FBI_Code:chararray,Year:chararray);
remove_header = filter crime_data by $2 != 'Date';
gr = GROUP remove_header BY (Primary_Type,Year);
gen = FOREACH gr GENERATE FLATTEN(group) AS (crime, year), COUNT(remove_header.Primary_Type) as crime_count;
fil = filter gen by $0 == 'THEFT';
orders = ORDER gen BY $0 ASC, $1 DESC;
STORE orders into 'secondary_sort' using PigStorage(',');
