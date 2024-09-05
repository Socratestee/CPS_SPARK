# Importing necessary dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, concat_ws, lit, date_format, to_date, concat, col, when, abs, sum

# Initiate Spark Session
spark = SparkSession.builder.appName('CPSDataAnalysis').getOrCreate()

# Load the Raw .dat file
data_path = r'C:\Users\user\Downloads\dec17pub\dec17pub.dat'
cps_df = spark.read.text(data_path)



# Extract HouseholdID
cps_df = cps_df.withColumn("household_id", cps_df["value"].substr(1, 15))


# Extract Interview Time

# Extract year and month from the `value` column
cps_df = cps_df.withColumn(
    "year", substring("value", 18, 4)  
).withColumn(
    "month", substring("value", 16, 2)
)

# Convert the extracted month number to an abbreviated month name
cps_df = cps_df.withColumn(
    "month_name", date_format(to_date(concat(lit("01"), col("month"), col("year")), "ddMMyyyy"), "MMM")
)

# Combine year and month abbreviation to get YYYY/MMM format
cps_df = cps_df.withColumn(
    "interview_time", concat_ws("/", col("year"), col("month_name"))
)


# Extract Final outcome
cps_df = cps_df.withColumn("final_outcome", cps_df["value"].substr(24, 3))



#Extract Type of Housing unit
cps_df = cps_df.withColumn("type_of_housing_unit", cps_df["value"].substr(31, 2))



# Extract Household Type
cps_df = cps_df.withColumn("household_type", cps_df["value"].substr(61, 2))



# Extract Apartment/Household has a telephone.
cps_df = cps_df.withColumn("household_has_telephone", cps_df["value"].substr(33, 2))

# Replace negative values with null as there are multiple negative values
cps_df = cps_df.withColumn(
    "household_has_telephone",
    when(cps_df["household_has_telephone"] < 0, None)
    .otherwise(cps_df["household_has_telephone"])
)



# Extract Apartment/Household can access a telephone elsewhere
cps_df  = cps_df.withColumn("household_can_access_telephone_elsewhere", cps_df["value"].substr(35, 2))

# Replace negative values with null as there are multiple negative values
cps_df = cps_df.withColumn(
    "household_can_access_telephone_elsewhere",
    when(cps_df["household_can_access_telephone_elsewhere"] < 0, None)
    .otherwise(cps_df["household_can_access_telephone_elsewhere"])
)



# Extract Is telephone interview acceptable for the responder
cps_df = cps_df.withColumn("telephone_interview_acceptable", cps_df["value"].substr(37, 2))

# Replace 0 with 1 and 1 with 2 since all the values are either 0 and 1
cps_df = cps_df.withColumn(
    "telephone_interview_acceptable",
    when(cps_df["telephone_interview_acceptable"] == 0, 1)
    .when(cps_df["telephone_interview_acceptable"] == 1, 2)
    .otherwise(cps_df["telephone_interview_acceptable"])
)



# Extract Type of interview
cps_df = cps_df.withColumn("type_of_interview", cps_df["value"].substr(65, 2))
# Replace negative values with absolute values since the negatives are all -1 which should be a data input error
cps_df = cps_df.withColumn(
    "type_of_interview",
    when(cps_df["type_of_interview"] < 0, abs(cps_df["type_of_interview"]).cast("Integer"))
    .otherwise(cps_df["type_of_interview"])
)



# Extract Family income range
cps_df = cps_df.withColumn("family_income_range", cps_df["value"].substr(39, 2))

# Replace negative values with absolute values since the negatives are all -1 which is taken as a data input error
cps_df = cps_df.withColumn(
    "family_income_range",
    when(cps_df["family_income_range"] < 0, abs(cps_df["family_income_range"]).cast("Integer"))
    .otherwise(cps_df["family_income_range"])
)


# Ensure family_income_range is consistently treated as an integer
cps_df = cps_df.withColumn("family_income_range", col("family_income_range").cast("Integer"))

# Group by family_income_range and sum the counts for any duplicate entries
income_count_df = cps_df.groupBy("family_income_range").count()

# Perform aggregation to sum any duplicate counts
income_count_df = income_count_df.groupBy("family_income_range").agg(sum("count").alias("count"))



# Extract Geographical division/location
cps_df = cps_df.withColumn("geographical_division", cps_df["value"].substr(91, 1))

# Extract Race
cps_df = cps_df.withColumn("race", cps_df["value"].substr(139, 2))

# Replace negative values with absolute values since the negatives are all -1 which should be a data input error
cps_df = cps_df.withColumn(
    "race",
    when(cps_df["race"] < 0, abs(cps_df["race"]).cast("Integer"))
    .otherwise(cps_df["race"])
)

cps_df = cps_df.drop("value", "year", "month", "month_name") #dropping columns not needed


# Q1
# Count of Responders per Family Income Range
income_count_df = cps_df.groupBy("family_income_range").count()

# Q2
# Count of Responders per Geographical Division/Location and Race
geo_race_count_df = cps_df.groupBy("geographical_division", "race").count().orderBy("count", ascending=False)


#Q3
# Responders Without a Telephone But Can Access Elsewhere and Accept Telephone Interview
no_telephone_but_access_accepted_df = cps_df.filter(
    (cps_df["household_has_telephone"] == "No") &
    (cps_df["household_can_access_telephone_elsewhere"] == "Yes") &
    (cps_df["telephone_interview_acceptable"] == "Yes")
).count()


#Q4
# Responders Can Access Telephone but Do Not Accept Telephone Interview
access_but_not_accepted_df = cps_df.filter(
    (cps_df["household_can_access_telephone_elsewhere"] == "Yes") &
    (cps_df["telephone_interview_acceptable"] == "No")
).count()


# Save the results to CSVs

income_count_df.write.csv(r"C:\Users\user\Documents\Data_engineering\Projects\family_income_count.csv", header=True)
geo_race_count_df.write.csv(r"C:\Users\user\Documents\Data_engineering\Projects\geo_race_count.csv", header=True)
no_telephone_but_access_accepted_df.write.csv(r"C:\Users\user\Documents\Data_engineering\Projects\no_telephone_but_access_accepted_df", header=True)
access_but_not_accepted_df.write.csv(r"C:\Users\user\Documents\Data_engineering\Projects\access_but_not_accepted_df", header=True)

# End Spark Session
spark.stop()