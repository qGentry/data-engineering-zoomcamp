## Question 3: Count records  (Multiple choice)
### How many taxi trips were totally made on January 15?
    select count(*) 
    from yellow_taxi_trips
    where lpep_pickup_datetime::date = to_date('2019-01-15', 'yyyy-mm-dd') and lpep_dropoff_datetime::date = to_date('2019-01-15', 'yyyy-mm-dd');

## Question 4: Largest trip for each day (Multiple choice)
### Which was the day with the largest trip distance?

    select max(trip_distance) as max_distance, lpep_dropoff_datetime 
    from yellow_taxi_trips 
    group by lpep_dropoff_datetime 
    order by max_distance desc 
    limit 1;

## Question 5: The number of passengers  (Multiple choice)
### In 2019-01-01 how many trips had 2 and 3 passengers?

    select count(*)
    from yellow_taxi_trips
    where lpep_pickup_datetime::date = to_date('2019-01-01', 'yyyy-mm-dd')  and passenger_count = 2;

    select count(*)
    from yellow_taxi_trips
    where lpep_pickup_datetime::date = to_date('2019-01-01', 'yyyy-mm-dd')  and passenger_count = 3;

## Question 6: Largest tip (Multiple choice)
### For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
Some cheating here - instead of joining table with id2name mappings, i've downloaded CSV from nyc taxi website and checked that "Astoria" -> id=7, id=146 -> "Long Island City/Queens Plaza".

id=146 was found using this query:

    select tip_amount, "DOLocationID" 
    from yellow_taxi_trips 
    where "PULocationID" = 7 order by tip_amount desc limit 1;