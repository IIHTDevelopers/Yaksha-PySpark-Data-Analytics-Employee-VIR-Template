import unittest
from pyspark.sql import SparkSession
from main import (
    load_and_cache_table,
    get_max_salary,
    get_mid_level_avg_salary,
    get_expert_count,
    get_highest_paid_employee,
    get_most_experienced_employee,
    get_average_salary,
    get_total_bonus,
    get_employee_skill_department
)
from test.TestUtils import TestUtils  # Assuming this exists and is functional


class PySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test MySQL to PySpark") \
            .config("spark.jars", "mysql-connector-java-8.0.12.jar") \
            .config("spark.driver.extraClassPath", "mysql-connector-java-8.0.12.jar") \
            .config("spark.executor.extraClassPath", "mysql-connector-java-8.0.12.jar") \
            .getOrCreate()

        cls.url = "jdbc:mysql://localhost:3306/employeedetails?useSSL=false"
        cls.user = "root"
        cls.password = "pass@word1"

        # Load and cache the required tables
        cls.employee_df = load_and_cache_table(cls.spark, cls.url, cls.user, cls.password, "Employee")
        cls.skills_df = load_and_cache_table(cls.spark, cls.url, cls.user, cls.password, "Skills")
        cls.experience_salary_df = load_and_cache_table(cls.spark, cls.url, cls.user, cls.password, "ExperienceSalary")
        cls.department_df = load_and_cache_table(cls.spark, cls.url, cls.user, cls.password, "Department")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_max_salary(self):
        """Test case for maximum salary"""
        try:
            actual_max_salary = get_max_salary(self.experience_salary_df)
            if actual_max_salary:
                expected_max_salary = 95000.00
                if expected_max_salary == actual_max_salary:
                    print("TestMaxSalary = Passed")
                    TestUtils.yakshaAssert("TestMaxSalary", True, "functional")
                else:
                    print(f"TestMaxSalary = Failed: Expected {expected_max_salary}, but got {actual_max_salary}")
                    TestUtils.yakshaAssert("TestMaxSalary", False, "functional")
            else:
                print("TestMaxSalary = Failed: Max salary is empty or missing")
                TestUtils.yakshaAssert("TestMaxSalary", False, "functional")
        except Exception as e:
            print(f"TestMaxSalary = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestMaxSalary", False, "functional")

    def test_avg_salary_mid_level(self):
        """Test case for average salary of Mid-level employees"""
        try:
            actual_avg_salary_mid_level = get_mid_level_avg_salary(self.experience_salary_df)
            if actual_avg_salary_mid_level:
                expected_avg_salary_mid_level = 74250.00
                if expected_avg_salary_mid_level == actual_avg_salary_mid_level:
                    print("TestAvgSalaryMidLevel = Passed")
                    TestUtils.yakshaAssert("TestAvgSalaryMidLevel", True, "functional")
                else:
                    print(f"TestAvgSalaryMidLevel = Failed: Expected {expected_avg_salary_mid_level}, but got {actual_avg_salary_mid_level}")
                    TestUtils.yakshaAssert("TestAvgSalaryMidLevel", False, "functional")
            else:
                print("TestAvgSalaryMidLevel = Failed: Average salary for Mid-level employees is empty or missing")
                TestUtils.yakshaAssert("TestAvgSalaryMidLevel", False, "functional")
        except Exception as e:
            print(f"TestAvgSalaryMidLevel = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestAvgSalaryMidLevel", False, "functional")

    def test_expert_count(self):
        """Test case for count of employees with expert proficiency"""
        try:
            actual_expert_count = get_expert_count(self.skills_df)
            if actual_expert_count:
                expected_expert_count = 3
                if expected_expert_count == actual_expert_count:
                    print("TestExpertCount = Passed")
                    TestUtils.yakshaAssert("TestExpertCount", True, "functional")
                else:
                    print(f"TestExpertCount = Failed: Expected {expected_expert_count}, but got {actual_expert_count}")
                    TestUtils.yakshaAssert("TestExpertCount", False, "functional")
            else:
                print("TestExpertCount = Failed: Expert count is empty or missing")
                TestUtils.yakshaAssert("TestExpertCount", False, "functional")
        except Exception as e:
            print(f"TestExpertCount = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestExpertCount", False, "functional")

    def test_highest_paid_employee(self):
        """Test case for highest-paid employee"""
        try:
            actual_name, actual_salary = get_highest_paid_employee(self.experience_salary_df, self.employee_df)
            if actual_name and actual_salary:
                expected_highest_paid_name = "David Brown"
                expected_highest_paid_salary = 95000.00
                if (expected_highest_paid_name == actual_name) and (expected_highest_paid_salary == actual_salary):
                    print("TestHighestPaidEmployee = Passed")
                    TestUtils.yakshaAssert("TestHighestPaidEmployee", True, "functional")
                else:
                    print(f"TestHighestPaidEmployee = Failed: Expected Name: {expected_highest_paid_name}, Salary: {expected_highest_paid_salary}, but got Name: {actual_name}, Salary: {actual_salary}")
                    TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "functional")
            else:
                print("TestHighestPaidEmployee = Failed: Highest paid employee details are empty or missing")
                TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "functional")
        except Exception as e:
            print(f"TestHighestPaidEmployee = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "functional")

    def test_most_experienced_employee(self):
        """Test case for most experienced employee"""
        try:
            actual_name, actual_years = get_most_experienced_employee(self.experience_salary_df, self.employee_df)
            if actual_name and actual_years:
                expected_most_experienced_name = "David Brown"
                expected_most_experienced_years = 10
                if (expected_most_experienced_name == actual_name) and (expected_most_experienced_years == actual_years):
                    print("TestMostExperiencedEmployee = Passed")
                    TestUtils.yakshaAssert("TestMostExperiencedEmployee", True, "functional")
                else:
                    print(f"TestMostExperiencedEmployee = Failed: Expected Name: {expected_most_experienced_name}, Years: {expected_most_experienced_years}, but got Name: {actual_name}, Years: {actual_years}")
                    TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "functional")
            else:
                print("TestMostExperiencedEmployee = Failed: Most experienced employee details are empty or missing")
                TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "functional")
        except Exception as e:
            print(f"TestMostExperiencedEmployee = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "functional")

    def test_average_salary(self):
        """Test case for average salary"""
        try:
            actual_avg_salary = get_average_salary(self.experience_salary_df)
            if actual_avg_salary:
                expected_avg_salary = 79000.00
                if expected_avg_salary == actual_avg_salary:
                    print("TestAverageSalary = Passed")
                    TestUtils.yakshaAssert("TestAverageSalary", True, "functional")
                else:
                    print(f"TestAverageSalary = Failed: Expected {expected_avg_salary}, but got {actual_avg_salary}")
                    TestUtils.yakshaAssert("TestAverageSalary", False, "functional")
            else:
                print("TestAverageSalary = Failed: Average salary is empty or missing")
                TestUtils.yakshaAssert("TestAverageSalary", False, "functional")
        except Exception as e:
            print(f"TestAverageSalary = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestAverageSalary", False, "functional")

    def test_total_bonus(self):
        """Test case for total bonus paid"""
        try:
            actual_total_bonus = get_total_bonus(self.experience_salary_df)
            if actual_total_bonus:
                expected_total_bonus = 41000.00
                if expected_total_bonus == actual_total_bonus:
                    print("TestTotalBonus = Passed")
                    TestUtils.yakshaAssert("TestTotalBonus", True, "functional")
                else:
                    print(f"TestTotalBonus = Failed: Expected {expected_total_bonus}, but got {actual_total_bonus}")
                    TestUtils.yakshaAssert("TestTotalBonus", False, "functional")
            else:
                print("TestTotalBonus = Failed: Total bonus is empty or missing")
                TestUtils.yakshaAssert("TestTotalBonus", False, "functional")
        except Exception as e:
            print(f"TestTotalBonus = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestTotalBonus", False, "functional")

    def test_employee_skill_department(self):
        """Test case for employee skill and department details"""
        try:
            result_df = get_employee_skill_department(self.employee_df, self.skills_df, self.department_df)
            if result_df:
                expected_columns = {"Name", "SkillName", "DepartmentName", "Location"}
                result_columns = set(result_df.columns)
                if expected_columns == result_columns:
                    print("TestEmployeeSkillDepartment = Passed")
                    TestUtils.yakshaAssert("TestEmployeeSkillDepartment", True, "functional")
                else:
                    print(f"TestEmployeeSkillDepartment = Failed: Expected columns {expected_columns}, but got {result_columns}")
                    TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "functional")
            else:
                print("TestEmployeeSkillDepartment = Failed: Employee skill and department details are empty or missing")
                TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "functional")
        except Exception as e:
            print(f"TestEmployeeSkillDepartment = Failed due to exception: {e}")
            TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "functional")


if __name__ == "__main__":
    unittest.main()
