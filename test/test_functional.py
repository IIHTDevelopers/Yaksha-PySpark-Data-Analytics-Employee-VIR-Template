import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum, udf
from pyspark.sql.types import StringType
from test.TestUtils import TestUtils
from main import *
class PySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test MySQL to PySpark") \
            .config("spark.jars", "mysql-connector-java-8.0.12.jar") \
            .getOrCreate()

        cls.url = "jdbc:mysql://localhost:3306/employeedetails?useSSL=false"
        cls.user = "root"
        cls.password = "admin"

        # Load and cache the required tables
        cls.employee_df = cls.load_and_cache_table("Employee")
        cls.skills_df = cls.load_and_cache_table("Skills")
        cls.experience_salary_df = cls.load_and_cache_table("ExperienceSalary")
        cls.department_df = cls.load_and_cache_table("Department")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @classmethod
    def load_and_cache_table(cls, table_name):
        """Load and cache a table from MySQL into a PySpark DataFrame."""
        df = cls.spark.read.format("jdbc") \
            .option("url", cls.url) \
            .option("dbtable", table_name) \
            .option("user", cls.user) \
            .option("password", cls.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        df.cache()  # Cache the table in memory
        return df

    @classmethod
    def get_max_salary(cls):
        """Get the maximum salary from the ExperienceSalary DataFrame."""
        return cls.experience_salary_df.agg(max("Salary").alias("Max_Salary")).collect()[0]["Max_Salary"]

    @classmethod
    def get_mid_level_avg_salary(cls):
        """Get the average salary for Mid-level employees."""
        def categorize_experience(years):
            if years < 3:
                return 'Junior'
            elif 3 <= years <= 6:
                return 'Mid-level'
            else:
                return 'Senior'

        categorize_udf = udf(categorize_experience, StringType())
        categorized_employees = cls.experience_salary_df.withColumn("ExperienceCategory",
                                                                    categorize_udf(col("YearsOfExperience")))
        return categorized_employees.filter(col("ExperienceCategory") == "Mid-level") \
            .agg(avg("Salary").alias("Average_Salary")).collect()[0]["Average_Salary"]

    @classmethod
    def get_expert_count(cls):
        """Get the count of employees with 'Expert' proficiency level."""
        return cls.skills_df.filter(col("ProficiencyLevel") == "Expert") \
            .groupBy("ProficiencyLevel").count().collect()[0]["count"]

    @classmethod
    def get_grace_taylor_hire_date(cls):
        """Get the hire date of Grace Taylor."""
        return cls.employee_df.filter(col("Name") == "Grace Taylor").select("HireDate").first()["HireDate"]

    @classmethod
    def get_highest_paid_employee(cls):
        """Get the highest-paid employee."""
        highest_paid_employee = cls.experience_salary_df.join(cls.employee_df, "EmployeeID") \
            .orderBy(col("Salary").desc()) \
            .select("Name", "Salary") \
            .first()
        return highest_paid_employee['Name'], highest_paid_employee['Salary']

    @classmethod
    def get_most_experienced_employee(cls):
        """Get the employee with the most years of experience."""
        most_experienced_employee = cls.experience_salary_df.join(cls.employee_df, "EmployeeID") \
            .orderBy(col("YearsOfExperience").desc()) \
            .select("Name", "YearsOfExperience") \
            .first()
        return most_experienced_employee['Name'], most_experienced_employee['YearsOfExperience']

    @classmethod
    def get_average_salary(cls):
        """Get the average salary across all employees."""
        return cls.experience_salary_df.agg(avg("Salary").alias("Average_Salary")).collect()[0]["Average_Salary"]

    @classmethod
    def get_total_bonus(cls):
        """Get the total bonus given to all employees."""
        return cls.experience_salary_df.agg(sum("Bonus").alias("Total_Bonus")).collect()[0]["Total_Bonus"]

    @classmethod
    def get_employee_skill_department(cls):
        """Get employees with their skills and department locations."""
        return cls.employee_df \
            .join(cls.skills_df, "EmployeeID") \
            .join(cls.department_df, "DepartmentID") \
            .select("Name", "SkillName", "DepartmentName", "Location")

    def test_max_salary(self):
        expected_max_salary = 95000.00
        actual_max_salary = self.get_max_salary()
        if expected_max_salary == actual_max_salary:
            print("TestMaxSalary = Passed")
            TestUtils.yakshaAssert("TestMaxSalary", True, "boundary")
        else:
            print(f"TestMaxSalary = Failed: Expected {expected_max_salary}, but got {actual_max_salary}")
            TestUtils.yakshaAssert("TestMaxSalary", False, "boundary")

    def test_avg_salary_mid_level(self):
        expected_avg_salary_mid_level = 74250.00
        actual_avg_salary_mid_level = self.get_mid_level_avg_salary()
        if expected_avg_salary_mid_level == actual_avg_salary_mid_level:
            print("TestAvgSalaryMidLevel = Passed")
            TestUtils.yakshaAssert("TestAvgSalaryMidLevel", True, "boundary")
        else:
            print(f"TestAvgSalaryMidLevel = Failed: Expected {expected_avg_salary_mid_level}, but got {actual_avg_salary_mid_level}")
            TestUtils.yakshaAssert("TestAvgSalaryMidLevel", False, "boundary")

    def test_expert_count(self):
        expected_expert_count = 3
        actual_expert_count = self.get_expert_count()
        if expected_expert_count == actual_expert_count:
            print("TestExpertCount = Passed")
            TestUtils.yakshaAssert("TestExpertCount", True, "boundary")
        else:
            print(f"TestExpertCount = Failed: Expected {expected_expert_count}, but got {actual_expert_count}")
            TestUtils.yakshaAssert("TestExpertCount", False, "boundary")



    def test_highest_paid_employee(self):
        expected_highest_paid_name = "David Brown"
        expected_highest_paid_salary = 95000.00
        actual_name, actual_salary = self.get_highest_paid_employee()
        if (expected_highest_paid_name == actual_name) and (expected_highest_paid_salary == actual_salary):
            print("TestHighestPaidEmployee = Passed")
            TestUtils.yakshaAssert("TestHighestPaidEmployee", True, "boundary")
        else:
            print(f"TestHighestPaidEmployee = Failed: Expected Name: {expected_highest_paid_name}, Salary: {expected_highest_paid_salary}, but got Name: {actual_name}, Salary: {actual_salary}")
            TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "boundary")

    def test_most_experienced_employee(self):
        expected_most_experienced_name = "David Brown"
        expected_most_experienced_years = 10
        actual_name, actual_years = self.get_most_experienced_employee()
        if (expected_most_experienced_name == actual_name) and (expected_most_experienced_years == actual_years):
            print("TestMostExperiencedEmployee = Passed")
            TestUtils.yakshaAssert("TestMostExperiencedEmployee", True, "boundary")
        else:
            print(f"TestMostExperiencedEmployee = Failed: Expected Name: {expected_most_experienced_name}, Years: {expected_most_experienced_years}, but got Name: {actual_name}, Years: {actual_years}")
            TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "boundary")

    def test_average_salary(self):
        expected_avg_salary = 79000.00
        actual_avg_salary = self.get_average_salary()
        if expected_avg_salary == actual_avg_salary:
            print("TestAverageSalary = Passed")
            TestUtils.yakshaAssert("TestAverageSalary", True, "boundary")
        else:
            print(f"TestAverageSalary = Failed: Expected {expected_avg_salary}, but got {actual_avg_salary}")
            TestUtils.yakshaAssert("TestAverageSalary", False, "boundary")

    def test_total_bonus(self):
        expected_total_bonus = 41000.00
        actual_total_bonus = self.get_total_bonus()
        if expected_total_bonus == actual_total_bonus:
            print("TestTotalBonus = Passed")
            TestUtils.yakshaAssert("TestTotalBonus", True, "boundary")
        else:
            print(f"TestTotalBonus = Failed: Expected {expected_total_bonus}, but got {actual_total_bonus}")
            TestUtils.yakshaAssert("TestTotalBonus", False, "boundary")

    def test_employee_skill_department(self):
        expected_columns = {"Name", "SkillName", "DepartmentName", "Location"}
        result_columns = set(self.get_employee_skill_department().columns)
        if expected_columns == result_columns:
            print("TestEmployeeSkillDepartment = Passed")
            TestUtils.yakshaAssert("TestEmployeeSkillDepartment", True, "boundary")
        else:
            print(f"TestEmployeeSkillDepartment = Failed: Expected columns {expected_columns}, but got {result_columns}")
            TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "boundary")

if __name__ == "__main__":
    unittest.main()
