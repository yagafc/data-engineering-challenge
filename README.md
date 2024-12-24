# Coding challenge - Data Engineer

Every step can be performed with whichever tool you like, unless specified.
1. Go on commoncrawl.org and download 3 segments of the most recent data (command
line)
2. Extract every external link you can find in a data structure of your choice
3. Load the data into a single column postgres table (postgres instance must be on docker)
4. Create a flag that indicates if the link redirects to a home page or a subsection of the
website
5. Aggregate by primary links and compute their frequency by also keeping track of
subsections
6. Create a column that specifies the country of the website
7. Create a column that categorizes the type of content hosted by the website by using an
external api, there are many services doing this.
8. Websites that could not be categorized by the previous step should have a flag that
indicates if the website is an ad-based domain. You can find many resources online for
this.
9. Compute at least 5 useful metrics you can think of based on both categorized and
uncategorized websites (subject to evaluation)
10. The final result must be saved in columnar (arrow is recommended) files following a
partition schema of your choosing (subject to evaluation)
11. Using an orchestration tool (airflow is recommended) build a parametric pipeline that can
be triggered on new segments of data. (the orchestration tool should also be docker
based). Parallelization wherever possible will be highly appreciated.
