from pyspark.sql import SparkSession

# Initialize Spark Session with MySQL connector jar
spark = SparkSession.builder \
    .appName("MySQL to PySpark") \
    .config("spark.jars", "mysql-connector-java-8.0.12.jar") \
    .getOrCreate()

# MySQL database connection details
url = "jdbc:mysql://localhost:3306/employeedetails1?useSSL=false"
user = "root"
password = "admin"

# Directly load the tables from MySQL without caching
employee_df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Employee") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

skills_df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Skills") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

experience_salary_df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "ExperienceSalary") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

department_df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "Department") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Placeholder functions
def get_max_salary(experience_salary_df):
    pass

def get_mid_level_avg_salary(experience_salary_df):
    pass

def get_expert_count(skills_df):
    pass

def get_highest_paid_employee(experience_salary_df, employee_df):
    return None, None
    pass

def get_most_experienced_employee(experience_salary_df, employee_df):
    return None, None
    pass

def get_average_salary(experience_salary_df):
    pass

def get_total_bonus(experience_salary_df):
    pass

def get_employee_skill_department(employee_df, skills_df, department_df):
    pass


get_max_salary(experience_salary_df)
get_mid_level_avg_salary(experience_salary_df)
get_expert_count(skills_df)
get_highest_paid_employee(experience_salary_df, employee_df)
get_most_experienced_employee(experience_salary_df, employee_df)
get_average_salary(experience_salary_df)
get_total_bonus(experience_salary_df)
get_employee_skill_department(employee_df, skills_df, department_df)

# example print statement

#print(f"Max Salary: {get_max_salary(experience_salary_df)}")


# Stop the Spark session
spark.stop()

