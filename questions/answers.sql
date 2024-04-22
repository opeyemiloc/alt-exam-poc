-- Part 2a
-- what is the most ordered item based on the number of times it appears in an order cart that checked out successfully?
-- Creating a CTE to filter successful orders and retrieve relevant information
-- Filter orders that have been successfully checked out
-- Join with line_items table to obtain item details for each order
-- Select only orders with 'success' status
-- Result: CTE 'successfulorders' contains order details of successfully checked out orders
with successful_orders as (
	select ord.order_id, ord.status, lit.item_id, lit.quantity 
	from alt_school.orders as ord
	join alt_school.line_items as lit on ord.order_id = lit.order_id
	where ord.status = 'success'
),
-- Calculating the number of times each product appears in successfully checked out orders
-- Count the number of times each item appears in successful orders
-- Grouping the results by item_id
-- Result: CTE 'productordersummary' contains the count of successful orders for each product
product_order_summary_count as (
	select so.item_id, count(*) as numb_of_times
	from successful_orders as so
	group by so.item_id
),
-- Assigning a rank to each product based on the total number of successful orders
-- Joining product summary with product details to obtain product names
-- Using dense rank to assign a rank based on the number of successful orders.
-- using dense rank because it can handle duplicates ranking. where there is a case of tie for a spot, ensuring both have same rank
-- Ordering the products by the number of successful orders in descending order
-- Result: CTE 'productorderrank' contains products ranked by the number of times a product appears on orders successfully checked out
product_order_rank as (
	select po.id as product_id,po.name as product_name, pos.numb_of_times, 
	dense_rank () over (order by pos.numb_of_times desc) as order_rank
	from product_order_summary_count as pos
	join alt_school.products po on po.id = pos.item_id
)
-- Selecting the product with the highest number of successful orders
-- Filtering the products to only include those with the highest rank (rank = 1)
-- Result: Final query output shows the most ordered product. 
--  	   Based on the number of times it appeared in customer's cart that checkedout successfullly
select product_id, product_name, numb_of_times as number_of_times
from product_order_rank
where order_rank = 1;


--  without considering currency, and without using the line_item table, find the top 5 spenders
-- non_visit_event CTE:
-- excluding the event visit ahead because its not relevant throughout the course of this analysis
-- This CTE filters out events of type 'visit' and assigns a unique row number to each event within the timeline of each customer. 
-- It selects relevant columns such as customer ID, event data, event timestamp, and assigns a row number based on event timestamp.
with non_visit_event as (
    select ev.customer_id, ev.event_data, ev.event_timestamp, 
    row_number()over(partition by ev.customer_id order by ev.event_timestamp) as row_num
    from alt_school.events ev
    where ev.event_data ->> 'event_type' != 'visit'
),
-- successful_checkout_check CTE:
-- This CTE determines the checkout status for each customer by analyzing their event timeline.
-- It checks if there is at least one successful checkout event for each customer and assigns a status flag accordingly.
-- The result includes the customer ID, event data, event timestamp, and a status flag indicating checkout status (1 for success, 0 for failure).
successful_checkout_check as (
    select nsv.customer_id, nsv.event_data, nsv.event_timestamp, 
    max(case when nsv.event_data ->> 'status' = 'success' then 1 else 0 end) over (partition by nsv.customer_id) as status
    from non_visit_event nsv
),
-- successful_checkouts CTE:
-- This CTE applies filter to return only customer that had a successful checkout process in the window of their timeline of activity.
-- I applied row_number and partitions based on customer_id to maintain the window customer seperation order.
-- The result includes the customer ID, event data, event timestamp, and row number.
successful_checkouts as (
    select scs.customer_id, scs.event_data, scs.event_timestamp,
    row_number() over (partition by scs.customer_id order by scs.event_timestamp) as row_num
    from successful_checkout_check scs
    where scs.status = 1
),
-- nonCheckout_event_track CTE:
-- This CTE filters out non-checkout events from successful checkout events.
-- It selects only those events that are not of type 'checkout', focusing on activities leading to successful checkout.
-- The result includes the customer ID, event data, and event timestamp.
nonCheckout_event_track as (
    select sc.customer_id, sc.event_data, sc.event_timestamp
    from successful_checkouts sc
    where sc.event_data ->> 'event_type' <> 'checkout'
),
-- items_added CTE:
-- This CTE calculates the total quantity of items added to the cart by each customer.
-- It aggregates the quantity of items added for each item ID by grouping them by customer ID.
-- The result includes the customer ID, item ID, and the total quantity of items added to the cart.
items_added as (
    select et.customer_id, et.event_data ->> 'item_id' as item_id, sum((et.event_data ->> 'quantity')::int) as total_added
    from nonCheckout_event_track et
    where et.event_data ->> 'event_type' = 'add_to_cart'
    group by et.customer_id, et.event_data ->> 'item_id'
),
-- items_removed CTE:
-- This CTE counts the total number of items removed from the cart by each customer.
-- It aggregates the count of items removed for each item ID by grouping them by customer ID.
-- The result includes the customer ID, item ID, and the total number of items removed from the cart.
items_removed as (
    select et.customer_id, et.event_data ->> 'item_id' as item_id, count(*) as total_removed
    from nonCheckout_event_track et
    where et.event_data ->> 'event_type' = 'remove_from_cart'
    group by et.customer_id, et.event_data ->> 'item_id'
),
-- total_purchased CTE:
-- This CTE calculates the total quantity of items purchased by each customer.
-- It computes the difference between the total quantity of items added and removed for each item ID to determine the total quantity purchased.
-- The result includes the customer ID, item ID, and the total quantity purchased.
total_purchased as (
    select ia.customer_id, ia.item_id, ia.total_added - coalesce(ir.total_removed, 0) as total_purchased
    from items_added ia
    left join items_removed ir on ia.customer_id = ir.customer_id and ia.item_id = ir.item_id
),
-- expenses CTE:
-- This CTE calculates the total amount spent by each customer.
-- It multiplies the total quantity of items purchased by their respective prices from the products table to calculate the total spend.
-- The result includes the customer ID and the total amount spent.
expenses as (
    select tp.customer_id, sum(tp.total_purchased * p.price) as total_spend
    from total_purchased tp
    join alt_school.products p on tp.item_id = p.id::text
    group by tp.customer_id
),
-- customer_expense_rank CTE:
-- customer table is joined with the expense cte created above to create a comprehensive table that gives detailed information.
-- This CTE ranks customers based on their total spend and retrieves the top 5 spenders.
-- using a dense_rank function to each customer based on their total spend, ensuring that if there're tied amount spent they would have same rank.
-- The result includes the customer ID, location, total spend, and spend rank.
customer_expense_rank as (
	select exps.customer_id, c.location, exps.total_spend, dense_rank() over (order by exps.total_spend desc) as spend_rank
	from expenses as exps
	join alt_school.customers c on exps.customer_id = c.customer_id
)
-- This query selects the customer ID, location, and total spend for the top 5 spenders based on their spend rank.
-- The result is limited to the top 5 spenders to provide a concise summary of the top spenders and their spending behavior.
select cer.customer_id as customer_id, cer.location as location,cer.total_spend as total_spend
from customer_expense_rank as cer
where cer.spend_rank <= 5;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- part 2b

-- using the events table, Determine **the most common location** (country) where successful checkouts occurred

-- Finding the most common location where successful checkouts occurred
-- Creating a CTE to store successful checkouts
-- steps taken to acheive this:
	-- selecting relevant columns
	--Filtering out event_data where event_type = 'checkout' and where event_data status = success
with successful_checkout as (
	select ev.customer_id, ev.event_data
	from alt_school.events as ev
	where ev.event_data ->> 'event_type' = 'checkout'
	and ev.event_data ->> 'status' = 'success'
),
-- Selecting the location and counting the number of successful checkouts for each location
-- the cte created above will be joined with location table on the customer id column that exists in the cte and the customer table
-- selecting relevant columns
-- Counting the number of successful checkouts
-- Grouping the results by location
-- using dense rank because it can handle duplicates ranking. where there is a case of tie for a spot, ensuring both have same rank
checkout_counts as (
	select cs.location, count(*) as checkout_count, dense_rank() over (order by count(*) desc) as location_rank
	from successful_checkout as sc
	join alt_school.customers as cs
	on sc.customer_id = cs.customer_id 
	group by "location"
)
-- Selecting the location with the highest number of successful checkout
-- Filtering the products to only include those with the highest rank (rank = 1)
-- Result: Final query output shows the highest ranked location. 
select location, checkout_count
from checkout_counts
where location_rank = 1;

-- using the events table, identify the customers who abandoned their carts and count the number of events (excluding visits) that occurred before the abandonment

-- "non_visit_event CTE" It filters out events of type 'visit' and assigns a row number to each event within each customer's timeline.
-- steps taken to acheive this:
	-- selecting relevant columns.
	-- Assigning a row number to each event within each customer's timeline.
	-- Ordering events by timestamp.
	-- Filtering out events of type 'visit'.
with non_visit_event as (
	select ev.customer_id, ev.event_data, ev.event_timestamp, 
	row_number () over(partition by ev.customer_id order by ev.event_timestamp)
	from alt_school.events ev
	where ev.event_data ->> 'event_type' != 'visit'
),
-- Checkout Status CTE: It determines the checkout status for each customer by checking if there is a successful checkout event in their timeline.
-- steps taken to acheive this:
	-- selecting relevant columns.
	-- Determining if the customer's checkout was successful
	-- Assigning a flag (1 for success, 0 for failure) to each customer's timeline
checkout_status as (
	select nsv.customer_id, nsv.event_data, nsv.event_timestamp,
	max(case when nsv.event_data ->> 'status' = 'success' then 1 else 0 end) over (partition by nsv.customer_id) as check_status
	from non_visit_event as nsv
),
-- just trying to get an ordered window of cusomer for visual check, consistency 
-- and making sure each customer window pertitions ends with the event_type checkout which indicates the end of activity
ordered_checkout as (
	select cst.customer_id, cst.check_status, cst.event_data, cst.event_timestamp,
	row_number() over (partition by cst.customer_id order by cst.event_timestamp)
	from checkout_status as cst	
),
-- FILTER OUT EVENTS WITH STATUS AS CANCELLED OR FAILED
-- SINCE IF IT WAS THE OPPOSITE TO CHECK ALL ACTIVITIES BEFORE AN ORDER WAS COMPLETED, YOU WILL NOT COUNT THE STATUS = SUCCESS DATA
-- SAME WAY THOSE OTHER STATUS SHOULD NOT BE COUNTED AS PART OF THE ACTVITIES, BUT AS A FINAL DESTINATION OF AN ACTIVTY
-- Abandoned Events CTE: It filters out customers who abandoned their cart by selecting those who did not have a successful checkout.
-- steps taken to acheive this:
	-- selecting relevant columns.
	-- Filtering out customers who did not have success status in their timeline of events
abandoned_events as (
    select os.customer_id, os.check_status, os.event_data, os.event_timestamp 
    from ordered_checkout as os
    where os.check_status = 0 and os.event_data ->> 'event_type' != 'checkout'
)
-- The Query below:
	-- Counts the number of events per unique customer before cart abandonment 
    -- its groups the results per each customer ID.
select ab.customer_id, count(*) as num_events
from abandoned_events as ab
group by ab.customer_id;

-- Find the average number of visits per customer, considering only customers who completed a checkout! return average_visits to 2 decimal place

-- CTE: visit_event
	-- 1. Selecting customer ID, event data, and event timestamp from the events table.
	-- 2. Using a window function to determine the check status based on the event data's success status.
with visit_event as (
	select ev.customer_id, ev.event_data, ev.event_timestamp, 
	max(case when ev.event_data ->> 'status' = 'success' then 1 else 0 end) over (partition by ev.customer_id) as check_status
	from alt_school.events ev
),
-- CTE: successful_events
-- 1. Filtering the results from the 'visit_event' CTE to include only customers who successfully checked out.
-- 2. Selecting customer ID, event data, event timestamp, and check status for these successful events.
successful_events as (
	select ve.customer_id, ve.event_data,ve.event_timestamp, ve.check_status 
	from visit_event as ve
	where ve.check_status = 1
)
-- Query to Calculate Average Number of Visits
-- 1. Counting the number of visits for each customer from the 'successful_events' CTE.
-- 2. Grouping the counts by customer ID.
-- 3. Calculating the average number of visits across all customers.
select round(avg(no_visits),2) as average_visits
from(
-- table subquery to get the Count of number of visits for each customer
	select se.customer_id, count(*) as no_visits
	from successful_events as se
	where se.event_data ->> 'event_type' = 'visit'
	group by se.customer_id
);