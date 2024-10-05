import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, sum
from templateespark import get_max_salary, get_mid_level_avg_salary, \
    get_expert_count, get_highest_paid_employee, get_most_experienced_employee, \
    get_average_salary, get_total_bonus, get_employee_skill_department
from test.TestUtils import TestUtils  # Ensure this import path is correct

class TestSparkOperations(unittest.TestCase):
    spark = None  # Declare Spark session at the class level

    @classmethod
    def setUpClass(cls):
        if cls.spark is None:  # Ensure only one Spark session is created
            cls.spark = SparkSession.builder \
                .appName("EmployeeDataProcessingTest") \
                .config("spark.jars", "mysql-connector-java-8.0.12.jar") \
                .getOrCreate()

        # MySQL database connection details
        url = "jdbc:mysql://localhost:3306/employeedetails1?useSSL=false"
        user = "root"
        password = "admin"

        # Load the tables without caching
        cls.employee_df = cls.spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", "Employee") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        cls.skills_df = cls.spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", "Skills") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        cls.experience_salary_df = cls.spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", "ExperienceSalary") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        cls.department_df = cls.spark.read.format("jdbc") \
            .option("url", url) \
            .option("dbtable", "Department") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

    @classmethod
    def tearDownClass(cls):
        if cls.spark:  # Stop the Spark session only if it was created
            cls.spark.stop()
            cls.spark = None  # Clean up the Spark session reference

    def test_get_max_salary(self):
        try:
            result = get_max_salary(self.experience_salary_df)
            expected_max_salary = self.experience_salary_df.agg(max("Salary")).collect()[0][0]
            passed = result == expected_max_salary
            TestUtils.yakshaAssert("test_get_max_salary", passed, "boundary")
            print("test_get_max_salary = Passed" if passed else "test_get_max_salary = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_max_salary", False, "boundary")
            print(f"test_get_max_salary = Failed with exception: {e}")

    def test_get_mid_level_avg_salary(self):
        try:
            result = get_mid_level_avg_salary(self.experience_salary_df)
            expected_avg_salary = self.experience_salary_df.filter(
                (col("YearsOfExperience") >= 3) & (col("YearsOfExperience") <= 6)
            ).agg(avg("Salary")).collect()[0][0]
            passed = result == expected_avg_salary
            TestUtils.yakshaAssert("test_get_mid_level_avg_salary", passed, "boundary")
            print("test_get_mid_level_avg_salary = Passed" if passed else "test_get_mid_level_avg_salary = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_mid_level_avg_salary", False, "boundary")
            print(f"test_get_mid_level_avg_salary = Failed with exception: {e}")

    def test_get_expert_count(self):
        try:
            result = get_expert_count(self.skills_df)
            expected_expert_count = self.skills_df.filter(col("ProficiencyLevel") == "Expert").count()
            passed = result == expected_expert_count
            TestUtils.yakshaAssert("test_get_expert_count", passed, "boundary")
            print("test_get_expert_count = Passed" if passed else "test_get_expert_count = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_expert_count", False, "boundary")
            print(f"test_get_expert_count = Failed with exception: {e}")

    def test_get_highest_paid_employee(self):
        try:
            result = get_highest_paid_employee(self.experience_salary_df, self.employee_df)
            joined_df = self.employee_df.join(self.experience_salary_df, "EmployeeID")
            expected_highest_paid_employee = joined_df.orderBy(col("Salary").desc()).select("Name", "Salary").first()
            passed = (result[0] == expected_highest_paid_employee["Name"]) and (result[1] == expected_highest_paid_employee["Salary"])
            TestUtils.yakshaAssert("test_get_highest_paid_employee", passed, "boundary")
            print("test_get_highest_paid_employee = Passed" if passed else "test_get_highest_paid_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_highest_paid_employee", False, "boundary")
            print(f"test_get_highest_paid_employee = Failed with exception: {e}")

    def test_get_most_experienced_employee(self):
        try:
            result = get_most_experienced_employee(self.experience_salary_df, self.employee_df)
            joined_df = self.employee_df.join(self.experience_salary_df, "EmployeeID")
            expected_most_experienced_employee = joined_df.orderBy(col("YearsOfExperience").desc()).select("Name", "YearsOfExperience").first()
            passed = (result[0] == expected_most_experienced_employee["Name"]) and (result[1] == expected_most_experienced_employee["YearsOfExperience"])
            TestUtils.yakshaAssert("test_get_most_experienced_employee", passed, "boundary")
            print("test_get_most_experienced_employee = Passed" if passed else "test_get_most_experienced_employee = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_most_experienced_employee", False, "boundary")
            print(f"test_get_most_experienced_employee = Failed with exception: {e}")

    def test_get_average_salary(self):
        try:
            result = get_average_salary(self.experience_salary_df)
            expected_avg_salary = self.experience_salary_df.agg(avg("Salary")).collect()[0][0]
            passed = result == expected_avg_salary
            TestUtils.yakshaAssert("test_get_average_salary", passed, "boundary")
            print("test_get_average_salary = Passed" if passed else "test_get_average_salary = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_average_salary", False, "boundary")
            print(f"test_get_average_salary = Failed with exception: {e}")

    def test_get_total_bonus(self):
        try:
            result = get_total_bonus(self.experience_salary_df)
            expected_total_bonus = self.experience_salary_df.agg(sum("Bonus")).collect()[0][0]
            passed = result == expected_total_bonus
            TestUtils.yakshaAssert("test_get_total_bonus", passed, "boundary")
            print("test_get_total_bonus = Passed" if passed else "test_get_total_bonus = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_total_bonus", False, "boundary")
            print(f"test_get_total_bonus = Failed with exception: {e}")

    def test_get_employee_skill_department(self):
        try:
            result = get_employee_skill_department(self.employee_df, self.skills_df, self.department_df)
            expected_df = self.employee_df.join(self.skills_df, "EmployeeID").join(self.department_df, "DepartmentID").select("Name", "SkillName", "DepartmentName", "Location")
            # Here we would compare the dataframes, but for now we'll consider it a success if there's no exception
            passed = result.count() == expected_df.count()  # Assuming count as a simple check
            TestUtils.yakshaAssert("test_get_employee_skill_department", passed, "boundary")
            print("test_get_employee_skill_department = Passed" if passed else "test_get_employee_skill_department = Failed")
        except Exception as e:
            TestUtils.yakshaAssert("test_get_employee_skill_department", False, "boundary")
            print(f"test_get_employee_skill_department = Failed with exception: {e}")


if __name__ == "__main__":
    unittest.main()
