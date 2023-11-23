
Newday Technical Exercise - Adam Noble
---
Welcome to my Newday Technical exercise, here you will find a list of dependencies to make this project work, as well as some additional notes.

First, the project consists of a few files:
- MovieRatingAnalysis.py - This is the main script that needs to be executed to generate the requested output files.
- Notebook Version - This script was developed in Jupyter, so the original file is there in the case that you want to see it in Notebook form.
- data - This directory holds the required source data for analysis.
- README.md - Well, you are here now, so it must be here.

PRE-REQUISITES
I have installed the following dependencies:

- Hadoop-3.3.6
- JDK-11 (outdated, I know, but it works!)
- Python 3.11
- Spark 3.5.0

TO EXECUTE
- Clone the git repo to a local directory
- Open cmd (assuming Windows OS)
- cd to the directory
- Run the line below:
spark-submit MovieRatingAnalysis.py

(given system environment variables are set correctly.)

FUTURE PROJECT PROGRESSION
- I would invest more time in building CI pipelines, so that any future commits will automatically be built and tested upon code commit. Out of experience, this would be Jenkins for me.
