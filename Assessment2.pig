/* Rohan van Wijk */

-- Load dataset using CSVExcel storage
ordersCSV = LOAD'/user/maria_dev/diplomacy/orders.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') AS
(game_id:int,
unit_id:int,
unit_order:chararray,
location:chararray,
target_dest:chararray,
success:int,
reason:int,
turn_num:int);

-- Select only target_dest Holland
filtered = FILTER ordersCSV BY target_dest == 'Holland';

-- Group by location
grouped = GROUP filtered BY location;

-- Counting target_dest
list_count = FOREACH grouped GENERATE group, COUNT(filtered);

-- Order alphabeticly
ordered = ORDER list_count BY $0 ASC;

-- show
DUMP ordered;
