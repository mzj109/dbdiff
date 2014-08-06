dbdiff
======
DB Diff.
DBA’s and DB developers who work with large amounts of data in databases will, I think, appreciate this.

DB Diff pertains to you if:

- You are working in a development environment (you’re not just a “Production” dba.
- You have fairly large datasets. You get define ‘large’ based on the peculiarities of your systems.
- You are asked to compare/diff data between to very large tables.
- Performing diffs is taking a long time — it’s tedious, it’s wasting your time — it’s wasting your system’s time.

- Enter DB Diff. The concept is:

- To ‘pour’ your data into Hadoop (using Sqoop and Avro). Think: table ABC from a Dev DB diffed against table ABC from a Prod DB.
- Now, write java to code to compare the the two tables, and, print out a report of all the differences.

I created a rudimentary hadoop (with avro) job to do this here: https://github.com/mzj109/dbdiff
I hope to properly comment/clean the code in the next few days, but there it is. It works.

The workflow:

- Use sqoop to import 2 mysql tables into hdfs (tables with the same structure).
- Tell the hadoop job the primary key column.
- Based on the PK column, it will diff every row.

