# calling the necessary packages
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)


# Clearing the memory
rm(list = ls())

## SUBJECT DATE
DATE_PARAM="2021-01-26"

date <- as.Date(DATE_PARAM, "%Y-%m-%d")

url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/",
  format(date, "%Y/%m/%d"), sep='')

wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}

# Save Raw Response and upload to S3
RAW_LOCATION_BASE = '/home/ozzy/Documents/CEU/ceu-data-platform-in-the-cloud-class/Homework 1/ceu-cloud-infra-homework-template/Data/raw-views/'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = FALSE)

# Save `wiki.response.body` to the local filesystem 

write.table(wiki.response.body, paste0(RAW_LOCATION_BASE, "/raw-views-", DATE_PARAM, ".txt" ))

#
# Upload the file you created to S3.
#
# Upload it to your bucket, place it under the folder called `de4/raw/` 
# The object name should be the same as your filename: `raw-edits-YYYY-MM-DD.txt`
# After you've uploaded it, make sure it's there

# by taking a look at the AWS Web Console
keyTable <- read.csv("/home/ozzy/Documents/CEU/ceu-data-platform-in-the-cloud-class/Homework 1/ceu-cloud-infra-homework-template/Keys/kaya_ozan@student.ceu.edu_accessKeys.csv",
                     header = T) # accessKeys.csv == the CSV downloaded from AWS containing your Acces & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 


BUCKET="ozan-ceu-2020"

## FILL IN AWS SETUP STEPS
put_object(file = paste0(RAW_LOCATION_BASE, "/raw-views-", DATE_PARAM, ".txt" ),
           object = paste0("de4/raw", "/raw-views-", DATE_PARAM, ".txt"),
           bucket = BUCKET,
           verbose = TRUE)





# Parse the response and write the parsed string to "Bronze"

# We are extracting the top edits from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
top.views = wiki.response.parsed$items[[1]]$articles


# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in top.views){
  record = list(
    article = page$article[[1]],
    views = page$views,
    rank = page$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top Edits JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='/home/ozzy/Documents/CEU/ceu-data-platform-in-the-cloud-class/Homework 1/ceu-cloud-infra-homework-template/Data/views/'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = FALSE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('de4/views/', 
                          json.lines.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)

