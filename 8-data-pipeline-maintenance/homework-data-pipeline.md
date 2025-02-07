# Data Pipeline Maintenance

## Pipeline Ownership

### Primary and Secondary Owners
1. **Profit Pipeline**
    - Primary Owner: Alice Johnson
    - Secondary Owner: Bob Smith

2. **Unit-level Profit for Experiments Pipeline**
    - Primary Owner: Carol White
    - Secondary Owner: Dave Brown

3. **Aggregate Profit Reported to Investors Pipeline**
    - Primary Owner: Bob Smith
    - Secondary Owner: Alice Johnson

4. **Growth Pipeline**
    - Primary Owner: Dave Brown
    - Secondary Owner: Carol White

5. **Aggregate Growth Reported to Investors Pipeline**
    - Primary Owner: Alice Johnson
    - Secondary Owner: Bob Smith

6. **Daily Growth for Experiments Pipeline**
    - Primary Owner: Carol White
    - Secondary Owner: Dave Brown

7. **Engagement Pipeline**
    - Primary Owner: Bob Smith
    - Secondary Owner: Alice Johnson

8. **Aggregate Engagement Reported to Investors Pipeline**
    - Primary Owner: Dave Brown
    - Secondary Owner: Carol White

## On-Call Schedule

### Weekly Rotation
- Week 1: Alice Johnson (Primary), Bob Smith (Secondary)
- Week 2: Bob Smith (Primary), Carol White (Secondary)
- Week 3: Carol White (Primary), Dave Brown (Secondary)
- Week 4: Dave Brown (Primary), Alice Johnson (Secondary)

### Holiday Considerations
- Ensure that each engineer gets at least one major holiday off (e.g., Christmas, New Year's Day).
- Swap shifts if an engineer has a planned vacation or personal event.

## Run Books for Investor Reporting Pipelines

### Aggregate Profit Reported to Investors Pipeline
1. **Data Sources**: Sales database, Expense database
2. **ETL Process**: Extract data daily at 2 AM, transform data to calculate aggregate profit, load data into the reporting database.
3. **Validation**: Check for data completeness and accuracy by comparing with previous day's data.
4. **Reporting**: Generate daily reports at 6 AM and send to investors via email.

### Aggregate Growth Reported to Investors Pipeline
1. **Data Sources**: User activity logs, Sales database
2. **ETL Process**: Extract data daily at 3 AM, transform data to calculate growth metrics, load data into the reporting database.
3. **Validation**: Ensure data consistency and validate against historical growth trends.
4. **Reporting**: Generate weekly growth reports every Monday at 7 AM and send to investors.

### Aggregate Engagement Reported to Investors Pipeline
1. **Data Sources**: User interaction logs, Feedback forms
2. **ETL Process**: Extract data daily at 4 AM, transform data to calculate engagement metrics, load data into the reporting database.
3. **Validation**: Cross-check engagement metrics with user feedback and interaction logs.
4. **Reporting**: Generate monthly engagement reports on the first of each month at 8 AM and send to investors.

## Potential Issues in Pipelines

1. **Data Inconsistency**: Mismatched data between different sources can lead to incorrect metrics.
2. **ETL Failures**: Failures in the ETL process can cause delays in data availability.
3. **Data Completeness**: Missing data can result in incomplete reports.
4. **Performance Bottlenecks**: Slow data processing can delay report generation.
5. **Security Breaches**: Unauthorized access to sensitive data can compromise data integrity.
6. **Hardware Failures**: Server or network failures can disrupt the pipeline operations.
7. **Software Bugs**: Bugs in the ETL scripts or reporting tools can lead to incorrect data processing.
8. **Human Errors**: Manual errors during data handling or report generation can affect the accuracy of the reports.

By addressing these potential issues and having a robust on-call schedule, we can ensure the smooth operation of our data pipelines and accurate reporting to investors.