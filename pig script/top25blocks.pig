crime_data = LOAD '/home/bhavik/Desktop/bigdata/Crimes.csv' USING PigStorage(',') as (ID:chararray,Case_Number:chararray,Date:chararray,Block:chararray,IUCR:chararray,Primary_Type:chararray,Description:chararray,Location_Description:chararray,Arrest:Boolean,Domestic:Boolean,Beat:chararray,District:chararray,Ward:chararray,Community_Area:chararray,FBI_Code:chararray,X_Coordinate::chararray,Y_Coordinate::chararray,Year:chararray,Updated_On:chararray,Latitude:chararray,Longitude:chararray,Location:chararray);
remove_header = filter crime_data by $2 != 'Date';
gr = GROUP remove_header BY Block;
gen = FOREACH gr GENERATE group as Block, COUNT(remove_header.Block);
orders = ORDER gen BY $1 DESC;
top_location = LIMIT orders 25;
STORE top_location into 'top25_final_block' using PigStorage(',');
