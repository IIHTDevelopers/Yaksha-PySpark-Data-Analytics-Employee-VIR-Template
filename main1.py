from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum, udf, broadcast
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# Path to MySQL connector JAR (ensure the path is correct)
mysql_jar_path = "mysql-connector-java-8.0.12.jar"

# Initialize Spark Session with configuration to use custom classpath for MySQL JAR
spark = SparkSession.builder \
    .appName("MySQL to PySpark") \
    .config("spark.driver.extraClassPath", mysql_jar_path) \
    .config("spark.executor.extraClassPath", mysql_jar_path) \
    .getOrCreate()

# MySQL database connection details
url = "jdbc:mysql://localhost:3306/employeedetails?useSSL=false"
user = "root"
password = "pass@word1"

def load_and_cache_table(spark, url, user, password, table_name):
    """Load and cache a table from MySQL into a PySpark DataFrame."""
    try:
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        df.cache()  # Cache the table in memory
        return df
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")
        return None

# Define UDF
def categorize_experience(years):
    """Categorizes employees by years of experience."""
    pass  # No implementation yet, return None by default
    return None

# Register the UDF for experience categorization
categorize_udf = udf(categorize_experience, StringType())

# Define data transformation functions with None as default return value
def get_max_salary(experience_salary_df):
    """Get the maximum salary from the ExperienceSalary DataFrame."""
    pass  # No implementation yet
    return None

def get_mid_level_avg_salary(experience_salary_df):
    """Get the average salary for Mid-level employees using PySpark SQL functions."""
    pass  # No implementation yet
    return None

def get_expert_count(skills_df):
    """Get the count of employees with 'Expert' proficiency level."""
    pass  # No implementation yet
    return None

def get_highest_paid_employee(experience_salary_df, employee_df):
    """Get the highest-paid employee."""
    pass  # No implementation yet
    return None, None

def get_most_experienced_employee(experience_salary_df, employee_df):
    """Get the employee with the most years of experience."""
    pass  # No implementation yet
    return None, None

def get_average_salary(experience_salary_df):
    """Get the average salary across all employees."""
    pass  # No implementation yet
    return None

def get_total_bonus(experience_salary_df):
    """Get the total bonus given to all employees."""
    pass  # No implementation yet
    return None

def get_employee_skill_department(employee_df, skills_df, department_df):
    """Get employees with their skills and department locations."""
    pass  # No implementation yet
    return None

# Load and cache the required tables
try:
    employee_df = load_and_cache_table(spark, url, user, password, "Employee")
    skills_df = load_and_cache_table(spark, url, user, password, "Skills")
    experience_salary_df = load_and_cache_table(spark, url, user, password, "ExperienceSalary")
    department_df = load_and_cache_table(spark, url, user, password, "Department")

    # Check if tables are successfully loaded
    if not (employee_df and skills_df and experience_salary_df and department_df):
        print("Failed to load one or more tables. Exiting.")
        spark.stop()
        exit(1)

    # Check if any DataFrame is empty
    if (employee_df.count() == 0 or skills_df.count() == 0 or
        experience_salary_df.count() == 0 or department_df.count() == 0):
        print("One or more tables have no data. Exiting.")
        spark.stop()
        exit(0)

    # Execute tasks (function calls)
    max_salary = get_max_salary(experience_salary_df)
    print(f"Max Salary: {max_salary if max_salary is not None else 'No Data'}")

    avg_mid_level_salary = get_mid_level_avg_salary(experience_salary_df)
    print(f"Average Salary for Mid-level Employees: {avg_mid_level_salary if avg_mid_level_salary is not None else 'No Data'}")

    expert_count = get_expert_count(skills_df)
    print(f"Total Employees with Expert Proficiency: {expert_count if expert_count is not None else 'No Data'}")

    highest_paid_name, highest_paid_salary = get_highest_paid_employee(experience_salary_df, employee_df)
    if highest_paid_name:
        print(f"Highest Paid Employee: {highest_paid_name} with Salary: {highest_paid_salary}")
    else:
        print("No highest-paid employee found.")

    most_experienced_name, most_experienced_years = get_most_experienced_employee(experience_salary_df, employee_df)
    if most_experienced_name:
        print(f"Most Experienced Employee: {most_experienced_name} with {most_experienced_years} years")
    else:
        print("No most experienced employee found.")

    avg_salary = get_average_salary(experience_salary_df)
    print(f"Average Salary: {avg_salary if avg_salary is not None else 'No Data'}")

    total_bonus = get_total_bonus(experience_salary_df)
    print(f"Total Bonus Paid: {total_bonus if total_bonus is not None else 'No Data'}")

    employee_skill_department = get_employee_skill_department(employee_df, skills_df, department_df)
    if employee_skill_department:
        print("Employees with their Skills and Department Locations:")
        employee_skill_department.show()
    else:
        print("No data available for employee skills and departments.")

except Exception as e:
    print(f"An error occurred during execution: {e}")

finally:
    # Stop the Spark session
    spark.stop()
