# Project: United States Immigration
## Datasets
<a href='https://travel.trade.gov/research/reports/i94/historical/2016.html'><b>I94 Immigration Data:</b></a>
- This data comes from the US National Tourism and Trade Office. 
- A data dictionary is included in the workspace. 
- There's a sample file so you can take a look at the data in csv format before reading it all in. 
- You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
- https://i94.cbp.dhs.gov/I94/#/home
- The immigration data and the global temperate data is in an attached disk.
- You can access the immigration data in a folder with the following path: ../../data/18-83510-I94-Data-**2016**/. 
- There's a file for each month of the year. An example file name is i94_apr16_sub.sas7bdat.

**What is a Form I-94?**
- Form I-94 is the DHS Arrival/Departure Record issued to aliens who are admitted to the U.S.,
- who are adjusting status while in the U.S. or extending their stay, among other things. 
- A CBP officer generally attaches the I-94 to the non-immigrant visitor's passport upon U.S. entry.
<br><br>

<a href='https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/'><b>U.S. City Demographic Data:</b></a>
- This data comes from OpenSoft. 
- This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
- This data comes from the US Census Bureau's **2015** American Community Survey.
<br><br>

***

## Schema Used For Analysis

Using the immigration and demographics datasets, we will to create a star schema optimized for queries on immigration analysis. This includes the following tables.

### Fact Table

- 1. immigration 

| N | Col | Description|
| --- | ---: | :---| 
|1|cicid|Application number / Citizenship and Immigration C...|
|**2**|**arrival_year**|**Arrival Year**|
|**3**|**arrival_month**|**Arrival Month**|
|4|citizinship|Country Immigrant is Originally From (country of citizernship)|
|5|residence|Country of Immigrant Residence|
|6|port|AIR / SEAPORT of entry into the US<br> ('XXX': 'NOT REPORTED/UNKNOWN' - '888': 'UNIDENTIFED AIR / SEAPORT' -'UNK': 'UNKNOWN POE')|
|**7**|**arrival_date**|**Arrival Date to USA**|
|8|travel_mode| (1: 'Air' - 2: 'Sea' - 3: 'Land' -  9: 'Not reported') |
|9|us_state|U.S. State / Address of Immigrant Inside USA <br> ('99'='All Other Codes') <br> actually representing the final address of the migrants, that is where they currently live in the US.|
|**10**|**departure_date**|**Departure Date from the USA**|
|**11**|**age**|**Age of Respondent in Years**|
|12|visa_category|Visa codes collapsed into three categories <br> (Business - Pleasure - Student)|
|13|dep_issued_visa|Department of State where where Visa was issued - CIC does not use <br> This is where your visa was issued. It will be a U.S. embassy or U.S. consulate.|
|**14**|**visa_expiration_date**|**Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use <br>  visa expiration date  <br>**|
|15|gender|Non-immigrant sex|
|16|airline|Airline used to arrive in U.S.|
|17|admission_number|Admission Number - An 11-digit number assigned to an alien when he enters the Unites States.|
|18|flight_number|Flight number of Airline used to arrive in U.S.|
|19|visa_type|VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|


  
### Dimension Tables

- 2. date - users in the app
     |-- arrdate: date (nullable = true)
     |-- arrival_day: integer (nullable = true)
     |-- arrival_week: integer (nullable = true)
     |-- arrival_month: integer (nullable = true)
     |-- arrival_year: integer (nullable = true)
     |-- arrival_weekday: integer (nullable = true)
- 3. demographics - songs in music database
     |-- City: string (nullable = true)
     |-- State: string (nullable = true)
     |-- median_age: double (nullable = true)
     |-- male_population: integer (nullable = true)
     |-- female_population: integer (nullable = true)
     |-- total_population: integer (nullable = true)
     |-- n_veterans: integer (nullable = true)
     |-- foreign_born: integer (nullable = true)
     |-- avg_household_size: double (nullable = true)
     |-- state_code: string (nullable = true)
     |-- Race: string (nullable = true)
     |-- Count: integer (nullable = true)


***
## Why Spark?
***
- Consdiering the significant size of the immigration dataset (~ 3 million rows) for only a month, the most sensible technology choice for such an approach would be spark, especially if we were to process data over a longer period of time.

- Apache spark was used because of:
    - it's ability to handle multiple file formats with large amounts of data.
    - Apache Spark offers a lightning-fast unified analytics engine for big data.
    - Spark has easy-to-use APIs for operating on large datasets


## Other Scenarios
**IF the data was increased by 100x**
- Spark can handle the increase but we would consider increasing the number of nodes in our cluster.
- We would still use spark as it as our data processing platform since it is the best suited platform for very large datasets.
- Our data would be stored in an Amazon S3 bucket (instead of storing it in the EMR cluster along with the staging tables) and loaded to our staging tables. 


**IF the data populates a dashboard that must be updated on a daily basis by 7am every day.**
- We would use Apache Airflow to schedule and run data pipelines.


**If the database needed to be accessed by 100+ people:**
- We would move our analytics database into Amazon Redshift
- Once the data is ready to be consumed, it would be stored in a postgres database on a redshift cluster that easily supports multiuser access.

***

