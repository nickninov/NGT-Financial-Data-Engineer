# Next Gate Tech - Financial Data Engineer - Take Home Assignment

## Objective
This assignment aims to assess your ability to solve real-world problems with technology, highlighting your problem-solving approach and the application of best practices.

## Problem Description
One of the most important principles in building a successful data management solution is ensuring data consistency and quality, which leads to a clean representation that can be leveraged by upstream services. Creating such a clean representation is challenging, as ingested data arrives in different formats, at asynchronous intervals, with varying identifiers and categorical values. The data may also be incomplete, sparse, or even incorrect at times. Additionally, it’s essential to establish a single source of truth to accurately link incoming data points to this clean version and avoid duplicates. Coupled with the need to handle a large volume of data points daily, and our goal to minimize or eliminate human intervention, the complexity of the problem becomes evident. In this assignment, your objective is to design and implement a simplified version of an application that ingests data from multiple sources and formats, harmonizes the data, enriches it with an external Financial API, and stores it in a well-structured database. Additionally, you will need to integrate a master referential database (golden source) to ensure onsistent mappings and data integrity across datasets.

## Take Breakdown

1. Data Ingestion & Harmonization
   - Create a data pipeline capable of ingesting and transforming data from multiple sources (see attached datasets Portfolio & Trades). The data may contain various inconsistencies, such as missing values, differing date formats, and inconsistent identifiers.
     - Setup a target database in which you can store the raw and cleaned outputs (can be local or hosted – see comments at the end).
     - Define which fields should be considered referential and which are non-referential, based on a method of your choice.
     - Ingest all non-referential fields into your designated database.
     - Ingest all referential fields into your designated database.
     - Implement a mechanism to avoid duplicating information and ensure uniqueness of the referential instruments, so we can access a clean and consistent representation of a particular instrument (e.g., Apple).
     - Design a smart approach to harmonize categorical variables, such as sector and country.
     - Hint: Don't spend too much time trying to harmonize all inconsistencies; focus on the most important aspects, such as date consistency.

2. Implement an extraction engine that leverages the [OpenFigi API](https://www.openfigi.com/api) to complement and enrich the referential database.
 - You will need to create an account to obtain API keys.
 - Use the `/v3/search` method to find the relevant instruments

3. Data Consumption
 - Create a data loader that can be used by other services or engineers to retrieve data from the database based on specific filters. The loader should handle the following use cases:
   - Load the historical time series for a particular instrument, using the identifier and field as arguments.
   - Load all historical time series prices for instruments listed in the United States.
  
4. Bonus: Human In The Loop
 - Incorporate a Human-in-the-Loop (HITL) mechanism into your solution to address cases where the data pipelines fail for any of the reasons mentioned above, or when there is insufficient information. Keep in mind that we aim to maintain a complete and clean referential database at all times.

Other Hints
 - Write your entire code in Python
 - The choice of database is not critical for this assignment, as we mainly need to see how the data is stored and loaded. You are free to choose any database; this could be a cloud-based option like those available on Google Cloud, a local database, or even a local Excel file.
 - Don't spend too much time on detailed data harmonization; focus on the most important aspects, such as date consistency.
 - Make sure to integrate data quality checks, ensuring both idempotency and the uniqueness of identifiers.

## Expected Deliverables & Submission

The expected deliverable is a GitHub repository containing your project code, and any other relevant files:
 - Please push the code of your entire project (make sure it's private) to GitHub and add the user `JohnExternalTester` as a contributor.
 - If you have deployed anything to Google Cloud Console, add john.external.tester.ngt@gmail.com with Viewer permission to the GCP project.

Feel free to ask any questions you might encounter during the process. Encouraging an open environment where colleagues share their work, communicate effectively, and feel comfortable asking questions is important to us.


## Additional Information
 - Feel free to use any publicly available resources or documentation.
 - You can use either GCP (preferred) or AWS.
 - This assignment is expected to take around 3 hours to complete (excluding the bonus tasks).
 - You have seven days to complete the assignment from the moment of receiving this document.

We look forward to seeing your solution and your approach to application development.

Good luck!
