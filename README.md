# Databricks-Vehicle-Collision-Data-Engineering
This project notebook contains comprehensive data engineering exercises using Databricks on the Motor Vehicle Collisions - Crashes dataset, focusing on data ingestion, cleaning, transformation, analysis, and visualization.

## Exercise 1: Data Ingestion and Cleaning
1. **Load the Data**: Load the dataset into a Databricks DataFrame.
2. **Schema Validation**: Verify that the schema matches the provided schema.
3. **Data Cleaning**: 
   - Remove any rows with null values in critical columns like `collision_id`, `crash_date`, and `crash_time`.
   - Convert `crash_date` and `crash_time` to appropriate date and time formats.
   - Handle any duplicate records based on `collision_id`.

## Exercise 2: Data Transformation
1. **Add New Columns**: 
   - Create a new column `crash_datetime` by combining `crash_date` and `crash_time`.
   - Extract the year, month, and day from `crash_datetime` into separate columns.
2. **Aggregate Data**: 
   - Calculate the total number of collisions per borough.
   - Find the average number of persons injured per collision for each borough.

## Exercise 3: Data Analysis
1. **Top Contributing Factors**: Identify the top 5 contributing factors for vehicle collisions.
2. **Collision Hotspots**: 
   - Determine the locations (latitude and longitude) with the highest number of collisions.
   - Visualize these hotspots on a map.

## Exercise 4: Data Visualization
1. **Time Series Analysis**: 
   - Plot the number of collisions over time (daily, monthly, yearly).
   - Analyze any trends or patterns.
2. **Injury and Fatality Analysis**: 
   - Create visualizations to show the distribution of injuries and fatalities among cyclists, motorists, and pedestrians.

## Additional Transformations
1. **Normalization**:
   - Normalize the borough column to ensure consistent naming (e.g., all lowercase or uppercase).
   - Standardize the `vehicle_type_code` columns to a common format.
2. **Geospatial Transformations**:
   - Convert latitude and longitude to a single geopoint column.
   - Calculate the distance between collision points and a central location (e.g., city center).
3. **Date and Time Transformations**:
   - Create a new column indicating the day of the week for each collision.
   - Categorize collisions into time slots (e.g., morning, afternoon, evening, night).
4. **String Manipulations**:
   - Extract street names from `on_street_name` and `cross_street_name` columns.
   - Create a new column that concatenates `on_street_name` and `cross_street_name` for a full address.
5. **Handling Missing Data**:
   - Impute missing values in `zip_code` based on the most frequent zip code for the corresponding borough.
   - Fill missing `contributing_factor_vehicle` columns with a default value like “Unknown”.
6. **Derived Metrics**:
   - Calculate the ratio of injured to killed persons for each collision.
   - Create a severity score based on the number of injuries and fatalities.
7. **Pivot and Unpivot**:
   - Pivot the dataset to have separate columns for each type of injury (cyclist, motorist, pedestrian).
   - Unpivot the dataset to have a single column for the type of injury and another for the count.
8. **Filtering and Subsetting**:
   - Filter collisions that occurred during weekends.
   - Create subsets of data for different vehicle types and analyze them separately.
9. **Window Functions**:
   - Use window functions to calculate the rolling average of collisions over a 7-day period.
   - Rank collisions within each borough based on the number of injuries.
