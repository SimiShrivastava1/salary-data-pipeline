"""
Salary normalization module for extracting and processing salary information from job descriptions.

Usage:
    - Import as a library: from salary import normalize_salary
    - Process entire CSV files: python salary/process_salary.py --input input_file.csv --output output_file.csv
    - Run tests: python salary/salarytest.py
    
Direct usage examples:
    - Basic usage: normalize_salary("$50,000 - $70,000 per year")
    - Hourly rate: normalize_salary("$20 - $25 per hour")
    - Monthly salary: normalize_salary("$4,000/month")
    - Single value: normalize_salary("$75,000 annually")
    - With K notation: normalize_salary("$80K annually")
    - Specific hours: normalize_salary("$20 an hour, 40 hours a week")

This module provides functions to extract standardized salary information from various
text formats and convert them to consistent yearly values.

"""

import re


def normalize_salary(salary_str: str, **kwargs) -> tuple:
    """
    Given a salary string in the form of a range, convert it to annual salary.
    This function finds the minimum, maximum, and average salary.

    Parameters:
    salary_str (str): The salary string in the form of a range.

    Returns:
    tuple: A tuple containing the minimum, maximum, and average salary as (min_salary, max_salary, avg_salary).
           Returns (None, None, None) if the string cannot be parsed.

    Supported formats:
    - Yearly: "$50,000 - $70,000 per year", "$50K annually", "$80K - $100K per year"
    - Monthly: "$4,000/month", "$4K-5K monthly"
    - Weekly: "$1,000 per week", "$1000-1200/wk"
    - Daily: "$200/day", "$200-300 daily"
    - Hourly: "$15 per hour", "$15-20/hr", "$20 an hour, 40 hours a week"
    - Per class/session: "$50 per class", "$100 per session"
    - Per student: "$25 per student"
    - Per mile: "$0.50 per mile"
    - Per sale/order/trip: "$50 per sale", "$10 per order", "$75 per trip"
    
    Special handling:
    - K notation: "K" is recognized as thousands (e.g., "$80K" = $80,000)
    - Specified hours: "40 hours a week" will override the default 40-hour assumption
    - Employment types: Handles text like "- Full-time", "- Part-time" after salary
    - High-value rates: Special handling for unusually high per-class, per-student, and per-mile rates

    Example:
    >>> normalize_salary("$50,000 - $70,000 per year")
    (50000.0, 70000.0, 60000.0)
    >>> normalize_salary("$20 per hour")
    (16640.0, 16640.0, 16640.0)
    >>> normalize_salary("$80K annually")
    (80000.0, 80000.0, 80000.0)
    >>> normalize_salary("$20 an hour, 40 hours a week")
    (41600.0, 41600.0, 41600.0)
    """
    if len(str(salary_str)) == 0:
        return None, None, None
    if str(salary_str).lower() == "nan":
        return None, None, None
    if not salary_str:
        return None, None, None
        
    # Clean the input, remove employment type information after the dash
    # Handle "- Full-time", "- Part-time", etc.
    employment_split = re.split(r'\s+-\s+(?:Full-time|Part-time|Contract|Seasonal|Temporary|Per diem|Temp-to-hire|Apprenticeship)', salary_str)
    if employment_split:
        salary_str = employment_split[0].strip()
    
    # Remove prefixes like "Up to", "From", etc.
    salary_str = re.sub(r'^(Up to|From)\s+', '', salary_str)
    
    # Handle "K" suffix first (convert to thousands before parsing numbers)
    # Look for patterns like 50K, 60.5K, etc.
    salary_str_temp = re.sub(r'(\d+\.?\d*)K', lambda x: str(float(x.group(1)) * 1000), salary_str)
    if salary_str_temp != salary_str:
        salary_str = salary_str_temp

    # Handle "M" suffix first (convert to millions before parsing numbers)
    # Look for patterns like 50M, 60.5M, etc.
    salary_str_temp = re.sub(r'(\d+\.?\d*)[Mm]', lambda x: str(float(x.group(1)) * 1000000), salary_str)
    if salary_str_temp != salary_str:
        salary_str = salary_str_temp
    
    # parse number
    salary_str = salary_str.replace(",", "").replace("$", "")
    
    # Check for hours per week pattern before extracting numbers
    hours_per_week = None
    hours_pattern = re.search(r'(\d+)\s+hours?\s+a\s+week', salary_str.lower())
    if hours_pattern:
        hours_per_week = float(hours_pattern.group(1))
        # Remove the hours per week part from the string to avoid it being detected as a salary value
        salary_str = re.sub(r'\d+\s+hours?\s+a\s+week', '', salary_str)
    
    # extract number
    salary = re.findall(r"\d+\.?\d*", salary_str)
    # convert to float
    salary = [float(i) for i in salary]
    # if no number found
    if len(salary) == 0:
        return None, None, None
    
    if len(salary) >= 2:
        min_salary = min(salary[0], salary[1])
        max_salary = max(salary[0], salary[1])
        salary[0], salary[1] = min_salary, max_salary
        
    # extract salary type
    salary_type = re.findall(r"hour|day|daily|week|month|year|class|session|game|student|sale|occurrence|mile|event|order|trip|annually", salary_str.lower())
    # if no salary type found return None
    if len(salary_type) == 0:
        return None, None, None
    
    # Convert 'annually' to 'year' for consistent processing
    if salary_type[0] == 'annually':
        salary_type[0] = 'year'
        
    # Identify the salary type to apply correct conversion
    salary_type = salary_type[0]
    
    # Handle abnormally high per-mile rates (likely data entry errors)
    if salary_type == "mile":
        # Check if any rate is abnormally high (typical mileage rates are $0.50-$2.00)
        for i in range(len(salary)):
            # If the rate is abnormally high (> $10 per mile), assume it's a mistake
            # and divide by 1000 to convert from cents to dollars
            if salary[i] > 5:
                salary[i] = salary[i] / 1000
            # If rate is too low (< $0.20), it might be a one-time fee rather than per-mile
            # Set minimum threshold
            elif salary[i] < 0.20:
                salary[i] = 0.20
    
    # Special handling for high per-student rates
    if salary_type == "student":
        # If the per-student rate is very high (> $500), assume it's already an annualized 
        # or total program rate rather than a per-student rate that needs multiplication
        high_student_rate = all(rate > 500 for rate in salary)
        if high_student_rate:
            # Return the values directly without applying the student multiplier
            if len(salary) == 1:
                return salary[0], salary[0], salary[0]
            else:
                min_val, max_val = salary[0], salary[1]
                avg_val = (min_val + max_val) / 2
                return min_val, max_val, avg_val
    
    # Special handling for high per-class rates (likely adjunct lecturer positions)
    if salary_type == "class":
        # If the per-class rate is very high (> $500), assume it's a semester course
        # and use a lower multiplier of 3 courses per year instead of 4*52
        high_class_rate = all(rate > 500 for rate in salary)
        if high_class_rate:
            # Use a multiplier of 3 classes per year for adjunct professors
            adjunct_multiplier = 3
            if len(salary) == 1:
                return salary[0] * adjunct_multiplier, salary[0] * adjunct_multiplier, salary[0] * adjunct_multiplier
            else:
                min_val = salary[0] * adjunct_multiplier
                max_val = salary[1] * adjunct_multiplier
                avg_val = (min_val + max_val) / 2
                return min_val, max_val, avg_val
    
    # Special handling for high per-session rates
    if salary_type == "session" and all(rate > 200 for rate in salary):
        # For high per-session rates, assume it's a recurring but less frequent event
        session_multiplier = 12  # Assume 1 session per month (12 per year)
        if len(salary) == 1:
            return salary[0] * session_multiplier, salary[0] * session_multiplier, salary[0] * session_multiplier
        else:
            min_val = salary[0] * session_multiplier
            max_val = salary[1] * session_multiplier
            avg_val = (min_val + max_val) / 2
            return min_val, max_val, avg_val
    
    # Skip processing for event and occurrence salary types
    if salary_type in ["event", "occurrence"]:
        return None, None, None
    
    # Multipliers for different salary types to convert to annual
    multipliers = {
        "hour": 8 * 5 * 52,  # 8 hours per day, 5 days per week, 52 weeks per year
        "day": 5 * 52,       # 5 days per week, 52 weeks per year
        "daily": 5 * 52,     # 5 days per week, 52 weeks per year
        "week": 52,          # 52 weeks per year
        "month": 12,         # 12 months per year
        "year": 1,           # Already annual
        "class": 4 * 52,     # Assuming 4 classes per week, 52 weeks
        "session": 3 * 52,   # Assuming 3 sessions per week, 52 weeks
        "game": 2 * 52,      # Assuming 2 games per week, 52 weeks
        "student": 20,       # Assuming 20 students per year
        "sale": 50,          # Assuming 50 sales per year (updated from 24)
        "mile": 45000,       # Assuming 45,000 miles per year for driver jobs (updated from 15,000)
        "order": 1000,       # Assuming 1000 orders per year
        "trip": 200          # Assuming 200 trips per year
    }
    
    # Get the standard multiplier first
    multiplier = multipliers.get(salary_type, 1)
    
    # Override hourly multiplier if we detected specific hours per week
    if hours_per_week is not None and salary_type == 'hour':
        # Replace the 8*5 part but keep the 52 weeks
        multiplier = hours_per_week * 52  # hours per week * weeks per year
    
    # Apply conversion based on number of salary figures
    if len(salary) == 1:
        min_salary = max_salary = salary[0] * multiplier
    else:
        min_salary = salary[0] * multiplier
        max_salary = salary[1] * multiplier
    
    avg_salary = (min_salary + max_salary) / 2
    
    return min_salary, max_salary, avg_salary

# Allow running as a script for simple testing
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Salary normalization utility for converting salary text to annual values.')
    parser.add_argument('salary', nargs='?', help='Salary string to normalize (e.g. "$50,000 - $70,000 per year")')
    parser.add_argument('--examples', action='store_true', help='Show usage examples')
    
    args = parser.parse_args()
    
    if args.examples:
        print("Example salary formats that can be normalized:")
        examples = [
            "$50,000 - $70,000 per year",
            "$20 - $25 per hour",
            "$4,000/month",
            "$1,000 per week",
            "$200/day",
            "$50 per class",
            "$25 per student",
            "$0.50 per mile"
        ]
        for example in examples:
            min_val, max_val, avg_val = normalize_salary(example)
            print(f"  Input: {example}")
            print(f"  Result: min=${min_val:,.2f}, max=${max_val:,.2f}, avg=${avg_val:,.2f}")
            print()
    elif args.salary:
        min_val, max_val, avg_val = normalize_salary(args.salary)
        if all(val is None for val in [min_val, max_val, avg_val]):
            print("Could not normalize the provided salary string.")
        else:
            print(f"Normalized annual salary:")
            print(f"  Minimum: ${min_val:,.2f}")
            print(f"  Maximum: ${max_val:,.2f}")
            print(f"  Average: ${avg_val:,.2f}")
    else:
        parser.print_help()