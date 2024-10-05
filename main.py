from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum, udf
from pyspark.sql.types import StringType

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

# Function definitions remain the same
def get_max_salary(experience_salary_df):
    """Get the maximum salary from the ExperienceSalary DataFrame."""
    return experience_salary_df.agg(max("Salary").alias("Max_Salary")).collect()[0]["Max_Salary"]


def get_mid_level_avg_salary(experience_salary_df):
    """Get the average salary for Mid-level employees."""

    def categorize_experience(years):
        if years < 3:
            return 'Junior'
        elif 3 <= years <= 6:
            return 'Mid-level'
        else:
            return 'Senior'

    categorize_udf = udf(categorize_experience, StringType())
    categorized_employees = experience_salary_df.withColumn("ExperienceCategory",
                                                            categorize_udf(col("YearsOfExperience")))
    return categorized_employees.filter(col("ExperienceCategory") == "Mid-level") \
        .agg(avg("Salary").alias("Average_Salary")).collect()[0]["Average_Salary"]


def get_expert_count(skills_df):
    """Get the count of employees with 'Expert' proficiency level."""
    return skills_df.filter(col("ProficiencyLevel") == "Expert") \
        .groupBy("ProficiencyLevel").count().collect()[0]["count"]


def get_highest_paid_employee(experience_salary_df, employee_df):
    """Get the highest-paid employee."""
    highest_paid_employee = experience_salary_df.join(employee_df, "EmployeeID") \
        .orderBy(col("Salary").desc()) \
        .select("Name", "Salary") \
        .first()
    return highest_paid_employee['Name'], highest_paid_employee['Salary']


def get_most_experienced_employee(experience_salary_df, employee_df):
    """Get the employee with the most years of experience."""
    most_experienced_employee = experience_salary_df.join(employee_df, "EmployeeID") \
        .orderBy(col("YearsOfExperience").desc()) \
        .select("Name", "YearsOfExperience") \
        .first()
    return most_experienced_employee['Name'], most_experienced_employee['YearsOfExperience']


def get_average_salary(experience_salary_df):
    """Get the average salary across all employees."""
    return experience_salary_df.agg(avg("Salary").alias("Average_Salary")).collect()[0]["Average_Salary"]


def get_total_bonus(experience_salary_df):
    """Get the total bonus given to all employees."""
    return experience_salary_df.agg(sum("Bonus").alias("Total_Bonus")).collect()[0]["Total_Bonus"]


def get_employee_skill_department(employee_df, skills_df, department_df):
    """Get employees with their skills and department locations."""
    return employee_df \
        .join(skills_df, "EmployeeID") \
        .join(department_df, "DepartmentID") \
        .select("Name", "SkillName", "DepartmentName", "Location")


# Execute tasks
print(f"Max Salary: {get_max_salary(experience_salary_df)}")
print(f"Average Salary for Mid-level Employees: {get_mid_level_avg_salary(experience_salary_df)}")
print(f"Total Employees with Expert Proficiency: {get_expert_count(skills_df)}")

highest_paid_name, highest_paid_salary = get_highest_paid_employee(experience_salary_df, employee_df)
print(f"Highest Paid Employee: {highest_paid_name} with Salary: {highest_paid_salary}")
most_experienced_name, most_experienced_years = get_most_experienced_employee(experience_salary_df, employee_df)
print(f"Most Experienced Employee: {most_experienced_name} with {most_experienced_years} years")
print(f"Average Salary: {get_average_salary(experience_salary_df)}")
print(f"Total Bonus Paid: {get_total_bonus(experience_salary_df)}")

employee_skill_department = get_employee_skill_department(employee_df, skills_df, department_df)
print("Employees with their Skills and Department Locations:")
employee_skill_department.show()

# Stop the Spark session
spark.stop()
