import unittest
from main import get_max_salary, get_mid_level_avg_salary, get_expert_count, get_highest_paid_employee, \
    get_most_experienced_employee, get_average_salary, get_total_bonus, get_employee_skill_department
from pyspark.sql import SparkSession
from TestUtils import TestUtils


class TestEmployeeDataFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestEmployeeDataFunctions") \
            .getOrCreate()

        # Mock data for the tests
        cls.experience_salary_df = cls.spark.createDataFrame([
            (1, 95000.0, 10, 5000),
            (2, 70000.0, 4, 3000),
            (3, 60000.0, 5, 2000),
            (4, 80000.0, 3, 4000)
        ], ["EmployeeID", "Salary", "YearsOfExperience", "Bonus"])

        cls.employee_df = cls.spark.createDataFrame([
            (1, "David Brown"),
            (2, "Alice Smith"),
            (3, "Bob Johnson"),
            (4, "Carol Davis")
        ], ["EmployeeID", "Name"])

        cls.skills_df = cls.spark.createDataFrame([
            (1, "Python"),
            (2, "SQL"),
            (3, "JavaScript"),
            (4, "Python")
        ], ["EmployeeID", "SkillName"])

        cls.department_df = cls.spark.createDataFrame([
            (1, "IT", "New York"),
            (2, "HR", "San Francisco"),
            (3, "Marketing", "Chicago"),
            (4, "IT", "New York")
        ], ["EmployeeID", "DepartmentName", "Location"])

    def test_get_max_salary(self):
        expected_max_salary = 95000.0
        actual_max_salary = get_max_salary(self.experience_salary_df)

        if expected_max_salary == actual_max_salary:
            print("TestMaxSalary = Passed")
            TestUtils.yakshaAssert("TestMaxSalary", True, "functional")
        else:
            print(f"TestMaxSalary = Failed: Expected {expected_max_salary}, but got {actual_max_salary}")
            TestUtils.yakshaAssert("TestMaxSalary", False, "functional")

    def test_get_mid_level_avg_salary(self):
        expected_avg_salary = 74250.0  # Assuming you calculated this value
        actual_avg_salary = get_mid_level_avg_salary(self.experience_salary_df)

        if expected_avg_salary == actual_avg_salary:
            print("TestMidLevelAvgSalary = Passed")
            TestUtils.yakshaAssert("TestMidLevelAvgSalary", True, "functional")
        else:
            print(f"TestMidLevelAvgSalary = Failed: Expected {expected_avg_salary}, but got {actual_avg_salary}")
            TestUtils.yakshaAssert("TestMidLevelAvgSalary", False, "functional")

    def test_get_expert_count(self):
        expected_expert_count = 3
        actual_expert_count = get_expert_count(self.skills_df)

        if expected_expert_count == actual_expert_count:
            print("TestExpertCount = Passed")
            TestUtils.yakshaAssert("TestExpertCount", True, "functional")
        else:
            print(f"TestExpertCount = Failed: Expected {expected_expert_count}, but got {actual_expert_count}")
            TestUtils.yakshaAssert("TestExpertCount", False, "functional")

    def test_get_highest_paid_employee(self):
        expected_highest_paid_name = 'David Brown'
        expected_highest_paid_salary = 95000.0
        result = get_highest_paid_employee(self.experience_salary_df, self.employee_df)

        if result:
            actual_name, actual_salary = result
            if (expected_highest_paid_name == actual_name) and (expected_highest_paid_salary == actual_salary):
                print("TestHighestPaidEmployee = Passed")
                TestUtils.yakshaAssert("TestHighestPaidEmployee", True, "functional")
            else:
                print(f"TestHighestPaidEmployee = Failed: Expected Name: {expected_highest_paid_name}, "
                      f"Salary: {expected_highest_paid_salary}, but got Name: {actual_name}, "
                      f"Salary: {actual_salary}")
                TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "functional")
        else:
            print("TestHighestPaidEmployee = Failed: Result is None")
            TestUtils.yakshaAssert("TestHighestPaidEmployee", False, "functional")

    def test_get_most_experienced_employee(self):
        expected_most_experienced_name = 'David Brown'
        expected_most_experienced_years = 10
        result = get_most_experienced_employee(self.experience_salary_df, self.employee_df)

        if result:
            actual_name, actual_years = result
            if (expected_most_experienced_name == actual_name) and (expected_most_experienced_years == actual_years):
                print("TestMostExperiencedEmployee = Passed")
                TestUtils.yakshaAssert("TestMostExperiencedEmployee", True, "functional")
            else:
                print(f"TestMostExperiencedEmployee = Failed: Expected Name: {expected_most_experienced_name}, "
                      f"Years: {expected_most_experienced_years}, but got Name: {actual_name}, "
                      f"Years: {actual_years}")
                TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "functional")
        else:
            print("TestMostExperiencedEmployee = Failed: Result is None")
            TestUtils.yakshaAssert("TestMostExperiencedEmployee", False, "functional")

    def test_get_average_salary(self):
        expected_avg_salary = 79000.0  # Assuming this is the calculated average salary
        actual_avg_salary = get_average_salary(self.experience_salary_df)

        if expected_avg_salary == actual_avg_salary:
            print("TestAverageSalary = Passed")
            TestUtils.yakshaAssert("TestAverageSalary", True, "functional")
        else:
            print(f"TestAverageSalary = Failed: Expected {expected_avg_salary}, but got {actual_avg_salary}")
            TestUtils.yakshaAssert("TestAverageSalary", False, "functional")

    def test_get_total_bonus(self):
        expected_total_bonus = 41000.0  # Assuming this is the calculated total bonus
        actual_total_bonus = get_total_bonus(self.experience_salary_df)

        if expected_total_bonus == actual_total_bonus:
            print("TestTotalBonus = Passed")
            TestUtils.yakshaAssert("TestTotalBonus", True, "functional")
        else:
            print(f"TestTotalBonus = Failed: Expected {expected_total_bonus}, but got {actual_total_bonus}")
            TestUtils.yakshaAssert("TestTotalBonus", False, "functional")

    def test_get_employee_skill_department(self):
        expected_data = [
            ("Alice Smith", "Python", "IT", "New York"),
            ("Bob Johnson", "SQL", "HR", "San Francisco"),
            ("Carol Davis", "JavaScript", "Marketing", "Chicago"),
            ("David Brown", "Java", "IT", "New York"),
            ("Eve White", "Excel", "Finance", "Boston"),
            ("Frank Green", "Cloud Computing", "Operations", "Seattle"),
            ("Grace Taylor", "Data Analysis", "HR", "San Francisco"),
            ("Hank Wilson", "Cybersecurity", "Marketing", "Chicago")
        ]

        actual_df = get_employee_skill_department(self.employee_df, self.skills_df, self.department_df)

        if actual_df is not None:
            actual_data = [(row['Name'], row['SkillName'], row['DepartmentName'], row['Location']) for row in
                           actual_df.collect()]
            if sorted(expected_data) == sorted(actual_data):
                print("TestEmployeeSkillDepartment = Passed")
                TestUtils.yakshaAssert("TestEmployeeSkillDepartment", True, "functional")
            else:
                print(f"TestEmployeeSkillDepartment = Failed: Expected {expected_data}, but got {actual_data}")
                TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "functional")
        else:
            print("TestEmployeeSkillDepartment = Failed: DataFrame is None")
            TestUtils.yakshaAssert("TestEmployeeSkillDepartment", False, "functional")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
