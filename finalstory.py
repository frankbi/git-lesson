"""STUFF FOR GA API"""
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, timedelta, datetime
import json


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'credentials.json'
VIEW_ID = '231006736'

#eventually, this will come from a user on Slack, but baby steps
ARTICLE_ID = '600288012'

#These are used in getPublishDate() to get the publish date of a story. You have to include a start and end date in a GA query.
TODAY = date.today()

YESTERDAY = TODAY - timedelta(days=1)
FOUR_DAYS_AGO = TODAY - timedelta(days=4)

START_DATE = FOUR_DAYS_AGO.strftime("%Y-%m-%d")
END_DATE = YESTERDAY.strftime("%Y-%m-%d")

"""STUFF FOR CHARTBEAT API"""
import time

import requests

from dotenv import load_dotenv
import os

load_dotenv()

apiKey = os.getenv('CHARTBEAT_API_KEY')

"""GOOGLE ANALYTICS DATA FUNCTIONS"""
#create an analytics object from Google Analytics
def initialize_analyticsreporting():
  print 
  
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics

#query Google Analytics to get the URL page path
def getPublishResponse(analytics, articleId):
  print("Entered get publish date")
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [
            {
              'startDate': 
              START_DATE, 
              'endDate': 
              END_DATE,
              }],
          'metrics': [{'expression': 'ga:uniquePageviews'}],
          'pageSize': 20,
          'dimensions': [{'name': 'ga:dimension25'}, {'name': 'ga:dimension52'}],
          'dimensionFilterClauses': [
            {
                'filters': [
                  {
                    "dimensionName": 'ga:dimension25',
                    "operator": 'EXACT',
                    "expressions": [
                      articleId
                    ]
                  }
                ]
            }
            
                
          ]
        }]
        
      }
  ).execute()

#pull the publishing date as a string from a Google Analytics JSON response
def getPublishDate(pubResponse):
  pubDate = pubResponse['reports'][0]['data']['rows'][0]['dimensions'][1]
  return pubDate

#convert a string into a date object
def convertDate(date):
    newDate = datetime.strptime(date, "%B %d, %Y").date()
    return newDate

#convert a date object into a string
def convertDateString(date):
  oldDate = date
  newDate = oldDate.strftime("%Y-%m-%d")
  return newDate

#sets the publication date of a story to be the "Start Date" for the Google Analytics query. returns a date object matching the publish date on the story.
def getStartDateObj(pubDate):
  startDateObj = convertDate(pubDate)
  return startDateObj

"""makes an End date object. The End date is either set to be 72 hours after the publish date, or to yesterday, depending on 
when the queried story was published."""
def getEndDateObj(startDateObj):
  print("Entered endDateObj")
  endDateObj = None
  today = date.today()
  potentialEnd = startDateObj + timedelta(days=3)
  print(potentialEnd)
  yesterday = today - timedelta(days=1)
  print("today is: ", today)
  endDateObj = None

  if today < potentialEnd:
    endDateObj = yesterday
    print("End date in if statement is:", endDateObj)
  else:
    endDateObj = startDateObj + timedelta(days=3)
    print("End date in else statement is:", endDateObj)

  return endDateObj

#converts a startDate object into a Start Date string
def getStartDateString(startDateObj):
  startDate = convertDateString(startDateObj)
  return startDate

#converts a endDate object into a Start Date string
def getEndDateString(endDateObj):
  endDate = convertDateString(endDateObj)
  return endDate

#gets the Total Sessions from the getTraffic() json response
def returnTotalSessions(response):
  totalSessions = response['reports'][0]['data']['totals'][0]['values'][0]
  return totalSessions

#get the traffic and the total sessions for an article 
def getTraffic(analytics, articleId, startDate, endDate):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [
            {
              'startDate': 
              startDate, 
              'endDate': 
              endDate,
              }],
          'metrics': [{'expression': 'ga:uniquePageviews'}],
          'pageSize': 20,
          'dimensions': [{'name': 'ga:dimension25'}, {'name': 'ga:channelGrouping'}],
          'dimensionFilterClauses': [
            {
                'filters': [
                  {
                    "dimensionName": 'ga:dimension25',
                    "operator": 'EXACT',
                    "expressions": [
                      articleId
                    ],
                  },
                ],
            },
          ],
        },
        ],
        'useResourceQuotas': True,
      }
  ).execute()

#get the unique pageviews and page path for the given story in the report.
#You need the page path to be able to run the Chartbeat query.
def getSessions(analytics, articleId, startDate, endDate):
    return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [
            {
              'startDate': 
              startDate, 
              'endDate': 
              endDate,
              }],
          'metrics': [{'expression': 'ga:uniquePageviews'}],
          'pageSize': 20,
          'dimensions': [{'name': 'ga:dimension25'}, {'name': 'ga:pagePath'}],
          'dimensionFilterClauses': [
            {
                'filters': [
                  {
                    "dimensionName": 'ga:dimension25',
                    "operator": 'EXACT',
                    "expressions": [
                      articleId
                    ]
                  }
                ]
            }
            
                
          ]
        }]
        
      }
  ).execute()

#extracts the page path from the JSON response in GA to use in the Chartbeat query.

def getPagePath(analytics, startDate, endDate):
  sessions_response = getSessions(analytics, ARTICLE_ID, startDate, endDate)
  print(sessions_response)
  
  #sets the pagePath based on the GA query
  pagePath = sessions_response['reports'][0]['data']['rows'][0]['dimensions'][1]

  if "/article/" in pagePath:
    pagePath = sessions_response['reports'][0]['data']['rows'][1]['dimensions'][1]

    if "/comments/" in pagePath:
      pagePath = sessions_response['reports'][0]['data']['rows'][2]['dimensions'][1]
      print("entered first comments if")
      print("Page Path is:", pagePath)
      
      return pagePath

  if "comments" in pagePath:
    pagePath = sessions_response['reports'][0]['data']['rows'][1]['dimensions'][1]

  print("Page Path is:", pagePath)
  return pagePath

"""CHARTBEAT AVG. ENGAGED TIME QUERY"""
def craftChartbeatQuery(apiKey, pagePath, startDate, endDate):
  with requests.Session() as s:
    submission = s.get(f"https://api.chartbeat.com/query/v2/submit/page/?apikey={apiKey}&host=startribune.com&sort-column=page_avg_time&sort_order=asc&start={startDate}&end={endDate}&metrics=page_avg_time&tz=America/Chicago&dimensions=path&limit=10&path={pagePath}")
    json = submission.json()

    queryId = json["query_id"]

  with requests.Session() as s:
      completed = False
      while not completed:
          status = s.get(f"https://api.chartbeat.com/query/v2/status/?apikey={apiKey}&host=startribune.com&query_id={queryId}")
          json = status.json()
          if json["status"] in ["completed", "downloaded"]:
              completed = True

              fetch = s.get(f"https://api.chartbeat.com/query/v2/fetch/?apikey={apiKey}&host=startribune.com&query_id={queryId}&format=json")
              data = fetch.json()
              print(data)
          time.sleep(10)

  return data

def craftCBMobileQuery(apiKey, pagePath, startDate, endDate):
  print("Mobile page path is:", pagePath)
  with requests.Session() as s:
    submission = s.get(f"https://api.chartbeat.com/query/v2/submit/page/?apikey={apiKey}&host=m.startribune.com&sort-column=page_avg_time&sort_order=asc&start={startDate}&end={endDate}&metrics=page_avg_time&tz=America/Chicago&dimensions=path&limit=10&path=m.startribune.com{pagePath}")
    json = submission.json()

    queryId = json["query_id"]

  with requests.Session() as s:
      completed = False
      while not completed:
          status = s.get(f"https://api.chartbeat.com/query/v2/status/?apikey={apiKey}&host=m.startribune.com&query_id={queryId}")
          json = status.json()
          if json["status"] in ["completed", "downloaded"]:
              completed = True

              fetch = s.get(f"https://api.chartbeat.com/query/v2/fetch/?apikey={apiKey}&host=m.startribune.com&query_id={queryId}&format=json")
              data = fetch.json()
              print(data)
          time.sleep(10)

  return data

def parseGAReport(GAResponse):
  print(GAResponse)
  sessions = GAResponse['reports'][0]['data']['totals'][0]['values'][0]

  direct = 0
  other = 0
  email = 0
  search = 0
  referral = 0
  social = 0

  #sort through the Traffic JSON to assign the correct traffic numbers to each variable
  for i in range(0, 6, 1):
    if i == 5 and (direct == 0 or other == 0 or email == 0 or search == 0 or referral == 0 or social == 0):
      break

    print("Top of loop, I is", i)
    if "Direct" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      direct = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("Direct traffic is:", direct)

    if "(Other)" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      other = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("(Other) traffic is:", other)

    if "Email" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      email = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("Email traffic is:", email)

    if "Organic Search" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      print("entered Search If")
      search = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("Organic Search traffic is:", search)

    if "Referral" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      referral = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("Referral traffic is:", referral)

    if "Social" in GAResponse['reports'][0]['data']['rows'][i]['dimensions'][1]:
      social = GAResponse['reports'][0]['data']['rows'][i]['metrics'][0]['values'][0]
      print("Social traffic is:", social)


  #create a dictionary of the Google Analytics data
  google_data = {
    "Sessions": sessions,
    "Direct": direct,
    "(Other)": other,
    "Email": email,
    "Organic Search": search,
    "Referral": referral,
    "Social": social
  }

  ga_json = json.dumps(google_data)

  return ga_json

def parseChartbeatReport(chartResponse):
  desktop_time = chartResponse['data'][0]['page_avg_time']

  minutes, seconds = convert_seconds_to_minutes_and_seconds(desktop_time)
  clock_format = format_time(minutes, seconds)

  desktop_time_dict = {
    "Desktop Avg. Engaged Time": clock_format
  }

  desktop_time_json = json.dumps(desktop_time_dict)

  return desktop_time_json

def parseMobileCBReport(CBMobile):
   mobile_time = CBMobile['data'][0]['page_avg_time']

   minutes, seconds = convert_seconds_to_minutes_and_seconds(mobile_time)
   clock_format = format_time(minutes, seconds)

   mobile_time_dict = {
      "Mobile Avg. Engaged Time": clock_format
   }

   mobile_time_json = json.dumps(mobile_time_dict)

   return mobile_time_json

def convert_seconds_to_minutes_and_seconds(seconds):
    minutes = seconds // 60
    remaining_seconds = seconds % 60

    return minutes, remaining_seconds

def format_time(minutes, seconds):
    formatted_minutes = str(minutes).zfill(2)
    formatted_seconds = str(seconds).zfill(2)

    return f"{formatted_minutes}:{formatted_seconds}"

#combines two json data points
def combine_json(json1, json2, json3):
    # Parse JSON strings into dictionaries
    data1 = json.loads(json1)
    data2 = json.loads(json2)
    data3 = json.loads(json3)

    # Merge the dictionaries
    merged_data = {**data1, **data2, **data3}

    # Convert merged dictionary back to JSON string
    combined_json = json.dumps(merged_data)

    return combined_json

def finalizeReport(analytics, apiKey):
  pub_response = getPublishResponse(analytics, ARTICLE_ID)
  pubDate = getPublishDate(pub_response)

  startDateObj = getStartDateObj(pubDate)
  endDateObj = getEndDateObj(startDateObj)

  #convert the Start and End Date objects into strings to be sent to GA.
  startDate = getStartDateString(startDateObj)
  endDate = getEndDateString(endDateObj)

  print("Start Date is:", startDate)
  print("End date is:", endDate)

  GAResponse = getTraffic(analytics, ARTICLE_ID, startDate, endDate)

  pagePath = getPagePath(analytics, startDate, endDate)

  CBResponse = craftChartbeatQuery(apiKey, pagePath, startDate, endDate)

  CBMobile = craftCBMobileQuery(apiKey, pagePath, startDate, endDate)
  print("CB Mobile output:", CBMobile)

  newGA = parseGAReport(GAResponse)
  print(newGA)

  newChartbeatDesktop = parseChartbeatReport(CBResponse)

  newChartbeatMobile = parseMobileCBReport(CBMobile)

  final_json = combine_json(newGA, newChartbeatDesktop, newChartbeatMobile)
  print("Final JSON is:", final_json)


def main():
  analytics = initialize_analyticsreporting()
  finalizeReport(analytics, apiKey)

if __name__ == '__main__':
  main()