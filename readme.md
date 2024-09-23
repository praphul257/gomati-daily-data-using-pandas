RUNNING THE APPLICATION

Step 1:
    Make a folder and add the provided .env file and docker-compose.yml file in that folder.

Step 2:
    Open Terminal and navigate to that folder and Run the below command:
    COMMAND: docker-compose up

It will read the docker compose file and will pull the required images and run the celery worker and beat.


NOW THE APPLICATION IS RUNNING...


FEATURE OF THE APP:

-- It will read a csv file having list of meters.
-- Then Fetch data from Gomati Database and after processing it will generate an output csv file of daily load and block load data.
-- Now the application will e-mail the file to the specified mail-ids in the .env file.


**CHECK THE .ENV FILE
