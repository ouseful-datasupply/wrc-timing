import click


import warnings
warnings.filterwarnings(
    action='ignore',
    category=UserWarning,
    module='pandas'
)


import requests
import re
import json
import os
import datetime

import sqlite3
from sqlite_utils import Database

import pandas as pd
from pandas.io.json import json_normalize

import isodate

import kml2geojson

#Dummy to allow for output via click
#display = print
display = click.echo

_SEEDED_TABLES = False

# Setup:

#This is used as a default in function definitions?
#Is there a tidier way of handling year default?
#Need a class?
YEAR = datetime.datetime.now().year

#The module carries state about the current rally
meta={ 'rallies': {}, 'rallies_metadata':{},
  'rallyId':None, 'stages':[],'championshipId':None }


#Call a resource by ID
wrcapi='https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byId?id=%22{}%22' #requires resource ID

#Need to clarify what stub goes with what root or base?
stubs = { 'url_root':'http://www.wrc.com/service/sasCacheApi.php?route={stub}',
          'url_base_pattern':'http://www.wrc.com/service/sasCacheApi.php?route=events/{SASEVENTID}/{{stub}}',
          'itinerary': 'rallies/{rallyId}/itinerary',
          'startlists': 'rallies/{rallyId}/entries',
         'penalties': 'rallies/{rallyId}/penalties',
         'retirements': 'rallies/{rallyId}/retirements',
         'stagewinners':'rallies/{rallyId}/stagewinners',
         'overall':'stages/{stageId}/results?rallyId={rallyId}',
         'split_times':'stages/{stageId}/splittimes?rallyId={rallyId}',
         'stage_times_stage':'stages/{stageId}/stagetimes?rallyId={rallyId}',
         'stage_times_overall':'stages/{stageId}/results?rallyId={rallyId}',
         'seasons':'seasons',
         'seasonDetails':'seasons/{seasonId}',
         'championship':'seasons/{seasonId}/championships/{championshipId}',
         'championship_results':'seasons/{seasonId}/championships/{championshipId}/results',
        }

#SQL in wrcResults.sql
SETUP_Q='''
CREATE TABLE "itinerary_event" (
  "eventId" INTEGER,
  "itineraryId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "priority" INTEGER
);
CREATE TABLE "itinerary_legs" (
  "itineraryId" INTEGER,
  "itineraryLegId" INTEGER PRIMARY KEY,
  "legDate" TEXT,
  "name" TEXT,
  "order" INTEGER,
  "startListId" INTEGER,
  "status" TEXT,
  FOREIGN KEY ("itineraryId") REFERENCES "itinerary_event" ("itineraryId")
);
CREATE TABLE "itinerary_sections" (
  "itineraryLegId" INTEGER,
  "itinerarySectionId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "order" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_stages" (
  "code" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "name" TEXT,
  "number" INTEGER,
  "stageId" INTEGER PRIMARY KEY,
  "stageType" TEXT,
  "status" TEXT,
  "timingPrecision" TEXT,
  "itineraryLegId" INTEGER,
  "itinerarySections.itinerarySectionId" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "itinerary_controls" (
  "code" TEXT,
  "controlId" INTEGER PRIMARY KEY,
  "controlPenalties" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "firstCarDueDateTime" TEXT,
  "firstCarDueDateTimeLocal" TEXT,
  "location" TEXT,
  "stageId" INTEGER,
  "status" TEXT,
  "targetDuration" TEXT,
  "targetDurationMs" INTEGER,
  "timingPrecision" TEXT,
  "type" TEXT,
  "itineraryLegId" INTEGER,
  "itinerarySections.itinerarySectionId" INTEGER,
  "roundingPolicy" TEXT,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);
CREATE TABLE "startlists" (
  "codriver.abbvName" TEXT,
  "codriver.code" TEXT,
  "codriver.country.countryId" INTEGER,
  "codriver.country.iso2" TEXT,
  "codriver.country.iso3" TEXT,
  "codriver.country.name" TEXT,
  "codriver.countryId" INTEGER,
  "codriver.firstName" TEXT,
  "codriver.fullName" TEXT,
  "codriver.lastName" TEXT,
  "codriver.personId" INTEGER,
  "codriverId" INTEGER,
  "driver.abbvName" TEXT,
  "driver.code" TEXT,
  "driver.country.countryId" INTEGER,
  "driver.country.iso2" TEXT,
  "driver.country.iso3" TEXT,
  "driver.country.name" TEXT,
  "driver.countryId" INTEGER,
  "driver.firstName" TEXT,
  "driver.fullName" TEXT,
  "driver.lastName" TEXT,
  "driver.personId" INTEGER,
  "driverId" INTEGER,
  "eligibility" TEXT,
  "entrant.entrantId" INTEGER,
  "entrant.logoFilename" TEXT,
  "entrant.name" TEXT,
  "entrantId" INTEGER,
  "entryId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "group.name" TEXT,
  "groupId" INTEGER,
  "group.groupId" INTEGER,
  "identifier" TEXT,
  "manufacturer.logoFilename" TEXT,
  "manufacturer.manufacturerId" INTEGER,
  "manufacturer.name" TEXT,
  "manufacturerId" INTEGER,
  "priority" TEXT,
  "status" TEXT,
  "tag" TEXT,
  "tag.name" TEXT,
  "tag.tagId" INTEGER,
  "tagId" INTEGER,
  "tyreManufacturer" TEXT,
  "vehicleModel" TEXT,
  "entryListOrder" INTEGER,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "roster" (
  "fiasn" INTEGER,
  "code" TEXT,
  "sas-entryid" INTEGER PRIMARY KEY,
  "roster_num" INTEGER,
  FOREIGN KEY ("sas-entryid") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "startlist_classes" (
  "eventClassId" INTEGER,
  "eventId" INTEGER,
  "name" TEXT,
  "entryId" INTEGER,
  PRIMARY KEY ("eventClassId","entryId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "penalties" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "penaltyDuration" TEXT,
  "penaltyDurationMs" INTEGER,
  "penaltyId" INTEGER PRIMARY KEY,
  "reason" TEXT,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "retirements" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "reason" TEXT,
  "retirementDateTime" TEXT,
  "retirementDateTimeLocal" TEXT,
  "retirementId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stagewinners" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "stageId" INTEGER,
  "stageName" TEXT,
  PRIMARY KEY ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId")
);
CREATE TABLE "stage_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "split_times" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "splitDateTime" TEXT,
  "splitDateTimeLocal" TEXT,
  "splitPointId" INTEGER,
  "splitPointTimeId" INTEGER PRIMARY KEY,
  "stageTimeDuration" TEXT,
  "stageTimeDurationMs" REAL,
  "startDateTime" TEXT,
  "startDateTimeLocal" TEXT,
  "stageId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_stage" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "position" INTEGER,
  "source" TEXT,
  "stageId" INTEGER,
  "stageTimeId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "championship_lookup" (
  "championshipId" INTEGER PRIMARY KEY,
  "fieldFiveDescription" TEXT,
  "fieldFourDescription" TEXT,
  "fieldOneDescription" TEXT,
  "fieldThreeDescription" TEXT,
  "fieldTwoDescription" TEXT,
  "name" TEXT,
  "seasonId" INTEGER,
  "type" TEXT,
  "_codeClass" TEXT,
  "_codeTyp" TEXT
);
CREATE TABLE "championship_results" (
  "championshipEntryId" INTEGER,
  "championshipId" INTEGER,
  "dropped" INTEGER,
  "eventId" INTEGER,
  "pointsBreakdown" TEXT,
  "position" INTEGER,
  "publishedStatus" TEXT,
  "status" TEXT,
  "totalPoints" INTEGER,
  PRIMARY KEY ("championshipEntryId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_codrivers" (
  "championshipEntryId" INTEGER PRIMARY KEY,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "tyreManufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_entries_manufacturers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" INTEGER,
  "Name" TEXT,
  "LogoFileName" TEXT,
  "Manufacturer" TEXT,
  "manufacturerId" INTEGER,
  "personId" TEXT,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_rounds" (
  "championshipId" INTEGER,
  "eventId" INTEGER,
  "order" INTEGER,
  PRIMARY KEY ("championshipId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_events" (
  "categories" TEXT,
  "clerkOfTheCourse" TEXT,
  "country.countryId" INTEGER,
  "country.iso2" TEXT,
  "country.iso3" TEXT,
  "country.name" TEXT,
  "countryId" INTEGER,
  "eventId" INTEGER PRIMARY KEY,
  "finishDate" TEXT,
  "location" TEXT,
  "mode" TEXT,
  "name" TEXT,
  "organiserUrl" TEXT,
  "slug" TEXT,
  "startDate" TEXT,
  "stewards" TEXT,
  "surfaces" TEXT,
  "templateFilename" TEXT,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "trackingEventId" INTEGER ,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_drivers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "tyreManufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "event_metadata" (
  "_id" TEXT,
  "availability" TEXT,
  "date-finish" TEXT,
  "date-start" TEXT,
  "gallery" TEXT,
  "hasdata" TEXT,
  "hasfootage" TEXT,
  "hasvideos" TEXT,
  "id" TEXT,
  "info-based" TEXT,
  "info-categories" TEXT,
  "info-date" TEXT,
  "info-flag" TEXT,
  "info-surface" TEXT,
  "info-website" TEXT,
  "kmlfile" TEXT,
  "logo" TEXT,
  "name" TEXT,
  "org-website" TEXT,
  "poi-Klo im Wald" TEXT,
  "poilistid" TEXT,
  "position" TEXT,
  "rosterid" TEXT,
  "sas-eventid" TEXT,
  "sas-itineraryid" TEXT,
  "sas-rallyid" TEXT,
  "sas-trackingid" TEXT,
  "sitid" TEXT,
  "testid" TEXT,
  "thumbnail" TEXT,
  "time-zone" TEXT,
  "tzoffset" TEXT,
  "year" INTEGER
);

CREATE TABLE "season" (
  "name" TEXT,
  "seasonId" INTEGER,
  "year" INTEGER,
  PRIMARY KEY ("seasonId")
);

CREATE TABLE "season_rounds" (
  "event.categories" TEXT,
  "event.clerkOfTheCourse" TEXT,
  "event.country.countryId" INTEGER,
  "event.country.iso2" TEXT,
  "event.country.iso3" TEXT,
  "event.country.name" TEXT,
  "event.countryId" INTEGER,
  "event.eventId" INTEGER,
  "event.finishDate" TEXT,
  "event.location" TEXT,
  "event.mode" TEXT,
  "event.name" TEXT,
  "event.organiserUrl" TEXT,
  "event.slug" TEXT,
  "event.startDate" TEXT,
  "event.stewards" TEXT,
  "event.surfaces" TEXT,
  "event.templateFilename" TEXT,
  "event.timeZoneId" INTEGER,
  "event.timeZoneName" INTEGER,
  "event.timeZoneOffset" INTEGER,
  "event.trackingEventId" INTEGER,
  "eventId" INTEGER,
  "order" INTEGER,
  "seasonId" INTEGER,
  FOREIGN KEY ("seasonId") REFERENCES "season" ("seasonId")
);

CREATE TABLE "season_championships" (
  "championshipId" INTEGER,
  "name" TEXT,
  "seasonId" INTEGER,
  "type" TEXT,
  FOREIGN KEY ("seasonId") REFERENCES "season" ("seasonId")
);
'''

SETUP_VIEWS_Q = '''
'''

def _getEventMetadata():
    ''' Get event metadata as JSON data feed from WRC API. '''
    url='https://webappsdata.wrc.com/srv/wrc/json/api/wrcsrv/byType?t=%22Event%22&maxdepth=1'
    eventmeta = requests.get(url).json()
    return eventmeta

def getEventMetadata():
    ''' Get a list of events from WRC as a flat pandas dataframe.
        Itinerary / event data is only available for rallies starting in stated year. '''
    eventMetadata = json_normalize(_getEventMetadata(),
                                   record_path='_meta',
                                   meta='_id'  ).drop_duplicates().pivot('_id', 'n','v').reset_index()

    eventMetadata['date-finish']=pd.to_datetime(eventMetadata['date-finish'])
    eventMetadata['date-start']=pd.to_datetime(eventMetadata['date-start'])
    eventMetadata['year'] = eventMetadata['date-start'].dt.year
    
    return eventMetadata


# TO DO - this all got out of hand; need to tidy up
def _getRallyMeta(year=YEAR):
    if year not in meta['rallies_metadata']:
      em= getEventMetadata()
      em = em[em['year']==year][['name','sas-rallyid', 'sas-eventid', 'kmlfile', 'date-start']].reset_index(drop=True).dropna()
      em['stub']=em['kmlfile'].apply(lambda x: x.split('_')[0])
      meta['rallies_metadata'][year] = em.set_index('stub').to_dict(orient='index')
    return meta['rallies_metadata'][year]

def getRallyIDs(year=YEAR):
    em = _getRallyMeta(year=year)
    return {k:em[k]['sas-rallyid'] for k in em}
    #return em[['stub','sas-rallyid']].set_index('stub').to_dict()['sas-rallyid']

def getEventIDs(year=YEAR):
    if year not in meta['rallies']:
      em = _getRallyMeta(year=year)
      meta['rallies'][year] = {k:em[k]['sas-eventid'] for k in em}
      #em[['stub','sas-eventid']].set_index('stub').to_dict()['sas-eventid']
    return meta['rallies'][year]


def listRallies(year=YEAR):
    return getRallyIDs(year)

def set_rallyId(rally, year=YEAR, rallyIDs=None):
    if rallyIDs is None:
        rallyIDs = getRallyIDs()
    if rally in rallyIDs:
        meta['rallyId']=rallyIDs[rally]
        meta['rally_name'] = rally
        #We're calling event metadata twice; tidy this up...
        meta['event_meta']= getEventMetadata().set_index('sas-rallyid').loc[meta['rallyId'],:].to_dict()
    return meta


#Utils
def nvToDict(nvdict, key='n',val='v', retdict=None):
    if retdict is None:
        retdict={nvdict[key]:nvdict[val]}
    else:
        retdict[nvdict[key]]=nvdict[val]
    return retdict
#assert nvToDict({'n': "id",'v': "adac-rallye-deutschland"}) == {'id': 'adac-rallye-deutschland'}

def _get_single_json_table(meta, stub):
    _json = requests.get( stubs['url_base'].format(stub=stubs[stub].format(**meta) ) ).json()
    return json_normalize(_json)

def _get_single_json_table_root(meta, stub):
    _json = requests.get( stubs['url_root'].format(stub=stubs[stub].format(**meta) ) ).json()
    return json_normalize(_json)

#Datagrab: roster
def _getRoster(roster_id):
    roster_json = requests.get(wrcapi.format(roster_id) ).json()
    roster=json_normalize(roster_json)
    
    aa=json_normalize(roster_json, record_path='_dchildren')
    zz=json_normalize(roster_json['_dchildren'],record_path=['_meta'], meta='_id').pivot('_id', 'n','v').reset_index()
    zz=pd.merge(zz,aa[['_id','name','type']], on='_id')[['fiasn','filename','sas-entryid','name']]
    zz.columns = ['fiasn','code','sas-entryid','roster_num']
    #defensive?
    zz = zz.dropna(subset=['sas-entryid'])
    return zz

def getRoster(meta):
    em = getEventMetadata()
    roster_id= em[em['sas-rallyid']==meta['rallyId']]['rosterid'].iloc[0]
    return _getRoster(roster_id)

#Datagrab: itinerary
def getItinerary(meta):
    ''' Get event itinerary. Also updates the stages metadata. '''
    itinerary_json=requests.get( stubs['url_base'].format(stub=stubs['itinerary'].format(**meta) ) ).json()
    itinerary_event = {}#json_normalize(itinerary_json).drop('itineraryLegs', axis=1)
    
    #meta='eventId' for eventId
    itinerary_legs = json_normalize(itinerary_json, 
                                    record_path='itineraryLegs').drop('itinerarySections', axis=1)
    #meta='eventId' for eventId
    itinerary_sections = json_normalize(itinerary_json,
                                        ['itineraryLegs', 'itinerarySections']).drop(['stages','controls'],axis=1)

    itinerary_stages=json_normalize(itinerary_json['itineraryLegs'],
                                    ['itinerarySections','stages'],
                                   meta=['itineraryLegId',['itinerarySections','itinerarySectionId']])
    meta['stageIds']=itinerary_stages['stageId'].tolist()
    #Should do this a pandas idiomatic way
    #meta['_stages']=zip(itinerary_stages['stageId'].tolist(),
     #                   itinerary_stages['code'].tolist(),
     #                   itinerary_stages['status'].tolist())
    meta['_stages'] = itinerary_stages[['stageId','code','status']].set_index('code').to_dict(orient='index')
    itinerary_controls=json_normalize(itinerary_json['itineraryLegs'], 
                                  ['itinerarySections','controls'] ,
                                     meta=['itineraryLegId',['itinerarySections','itinerarySectionId']])
    itinerary_controls['stageId'] = itinerary_controls['stageId'].fillna(-1).astype(int)
    
    return itinerary_event, itinerary_legs, itinerary_sections, itinerary_stages, itinerary_controls

#Datagrab: startlists
def get_startlists(meta):
    startlists_json=requests.get( stubs['url_base'].format(stub=stubs['startlists'].format(**meta) ) ).json()
    ff=[]
    for f in startlists_json:
        if f['manufacturer']['logoFilename'] is None:
            f['manufacturer']['logoFilename']=''
        if f['entrant']['logoFilename'] is None:
            f['entrant']['logoFilename']='' 
        ff.append(f)
    startlists = json_normalize(ff).drop('eventClasses', axis=1)
    startlist_classes = json_normalize(ff,['eventClasses'], 'entryId' )
    #startlists = json_normalize(startlists_json).drop('eventClasses', axis=1)
    #startlist_classes = json_normalize(startlists_json,['eventClasses'], 'entryId' )
    
    return startlists, startlist_classes 

#Datagrab: penalties
def get_penalties(meta):
    ''' Get the list of penalties for a specified event. '''
    penalties = _get_single_json_table(meta, 'penalties')
    return penalties


#Datagrab: retirements
def get_retirements(meta):
    ''' Get the list of retirements for a specified event. '''
    retirements = _get_single_json_table(meta, 'retirements')
    return retirements

#Datagrab: stagewinners
def get_stagewinners(meta):
    ''' Get the stage winners table for a specified event. '''
    stagewinners = _get_single_json_table(meta, 'stagewinners')
    return stagewinners


#Utils:
def _single_stage(meta2, stub, stageId):
    ''' For a single stageId, get the requested resource. '''
    meta2['stageId']=stageId
    _json=requests.get( stubs['url_base'].format(stub=stubs[stub].format(**meta2) ) ).json()
    _df = json_normalize(_json)
    _df['stageId'] = stageId
    return _df

def _stage_iterator(meta, stub, stage=None):
    ''' Iterate through a list of stageId values and get requested resource.
        By default, we use a list of *all* the stageIds. '''

    meta2={'rallyId':meta['rallyId']}
    df = pd.DataFrame()
    #If stage is None get data for all stages
    if stage=='':
        stages=[]
    elif stage:
        stages=[]
        #If we have a single stage (specified in form SS4) get it
        if isinstance(stage,str) and stage in meta['_stages']:
            stages.append(meta['_stages'][stage]['stageId'])
        #If we have a list of stages (in form ['SS4','SS5']) get them all
        elif isinstance(stage, list) or isinstance(stage, tuple):
            for _stage in stage:
                #Try to be sensitive: cope with stage (SS1 etc) or stageId
                if isinstance(_stage,str) and _stage in meta['_stages']:
                    stages.append(meta['_stages'][_stage]['stageId'])
                elif _stage in meta['stageIds']:
                    stages.append(_stage)
    else:
        stages = meta['stageIds']

    #Get data for required stages
    #A stage is required if:
    # - it is running;
    # - it is not ToRun and it is not in the database;
    # stages are the IDs, not human readable - need to rename
    #print('Grabbing', stages)
    for stageId in stages:
        _df = _single_stage(meta2, stub, stageId)
        df = pd.concat([df, _df], sort=False)
    return df.reset_index(drop=True)

#Datagrab: overall
def get_overall(meta, stage=None):
    ''' Get the overall results table for all stages on an event or a specified stage. '''
    stage_overall = _stage_iterator(meta, 'overall', stage)
    return stage_overall


#Datagrab: splitTimes
def get_splitTimes(meta, stage=None):
    ''' Get split times table for all stages on an event or a specified stage. '''
    split_times = _stage_iterator(meta, 'split_times', stage)
    return split_times


#Datagrab: stage_times_stage
def get_stage_times_stage(meta, stage=None):
    ''' Get stage times table for all stages on an event or a specified stage. '''
    stage_times_stage = _stage_iterator(meta, 'stage_times_stage', stage)
    return stage_times_stage


#Datagrab: stage_times_overall
def get_stage_times_overall(meta,stage=None):
    ''' Get overall stage times table for all stages on an event or a specified stage. '''
    stage_times_overall = _stage_iterator(meta, 'stage_times_overall', stage)
    return stage_times_overall


# +
#Datagrab: seasons

def get_seasons():
    ''' Get season info. '''
    # TO DO - this doesn't resolve?
    return json_normalize( requests.get(stubs['url_root'].format(stub=stubs['seasons'] )).json() )



# -

def get_seasonId(year=YEAR):
    ''' Get seasonId by year. '''
    df = get_seasons()
    return df.loc[df['year']==int(year),'seasonId'].iloc[0]

#Datagrab: seasonDetails
def get_season_details(seasonId):
    return requests.get(stubs['url_root'].format(stub=stubs['seasonDetails'].format(seasonId=seasonId) )).json()

def get_season_rounds(year=None, seasonId=None):
    if year is None and seasonId is None:
      year = YEAR

    if year is not None:
      seasonId = get_seasonId(YEAR)

    if seasonId is not None:
      return json_normalize( get_season_details(seasonId)['seasonRounds'] )
    else:
      return pd.DataFrame()

def get_season_championships(year=None, seasonId=None):
    ''' Get championships in a given season. '''

    if year is None and seasonId is None:
      year = YEAR

    if year is not None:
      seasonId = get_seasonId(year)

    if seasonId is not None:
      return json_normalize( get_season_details(seasonId)['championships'] )
    else:
      return pd.DataFrame()

def save_season_rounds(conn, year=YEAR):
    ''' Save season data to db. '''
    dbfy(conn, get_season_championships(year)[['championshipId','name', 'seasonId','type']], 'season_championships')
    dbfy(conn, get_seasons(), 'season')
    dbfy(conn, get_season_rounds(year), 'season_rounds')


#Datagrab: championship tables
# TO DO: rename, or extract into get_championship_rounds()?
#A function het_championship already exists
def championship_tables(champ_class=None, champ_typ=None, year=YEAR):
    ''' Get all championship tables in a particular championship and / or class. '''
    #if championship is None then get all
    championship_lookup = pd.DataFrame()
    championship_entries_all = {}
    championship_rounds = pd.DataFrame()
    championship_events = pd.DataFrame()
    championship_results = pd.DataFrame()
    
    seasonId = get_seasonId(year)
    
    championships = get_season_details(seasonId)['championships']
    
    for championship in championships:
        champ_num = championship['championshipId']
        #TO DO - are we setting the champType correctly?
        # championship['type'] returns as Person or Manufacturer
        champType = championship['name'].split()[-1]#championship['type']
        if champType not in championship_entries_all:
            championship_entries_all[champType] = pd.DataFrame()
            
        meta2={'championshipId': champ_num,
               'seasonId': seasonId}
        
        championship_url = stubs['url_root'].format(stub=stubs['championship'].format(**meta2) )
        championship_json=requests.get( championship_url ).json()
        if championship_json:
            _championship_lookup = json_normalize(championship_json).drop(['championshipEntries','championshipRounds'], axis=1)
            _championship_lookup['_codeClass'] = championship['name']
            _championship_lookup['_codeTyp'] = championship['type']
            championship_lookup = pd.concat([championship_lookup,_championship_lookup],sort=True)
    
            championships={}
            championship_dict = _championship_lookup.to_dict()
            championships[champ_num] = {c:championship_dict[c][0] for c in championship_dict}
            renamer={c.replace('Description',''):championships[champ_num][c] for c in championships[champ_num] if c.startswith('field')}            
            _championship_entries = json_normalize(championship_json,['championshipEntries'] )
            _championship_entries = _championship_entries.rename(columns=renamer)
            _championship_entries = _championship_entries[[c for c in _championship_entries.columns if c!='']]
            #pd.concat sort=False to retain current behaviour
            
            championship_entries_all[champType] = pd.concat([championship_entries_all[champType],_championship_entries],sort=False)

            _championship_rounds = json_normalize(championship_json,['championshipRounds'] ).drop('event', axis=1)
            championship_rounds = pd.concat([championship_rounds,_championship_rounds],sort=False).drop_duplicates()

            _events_json = json_normalize(championship_json,['championshipRounds' ])['event']
            _championship_events = json_normalize(_events_json)
            #Below also available as https://www.wrc.com/service/sasCacheApi.php?route=seasons/{seasonId}/championships/{championshipId}
            # eg https://www.wrc.com/service/sasCacheApi.php?route=seasons/4/championships/24
            championship_events = pd.concat([championship_events,_championship_events],sort=False).drop_duplicates()
            
            #Note that we use a different URL basis here
            _championship_results = _get_single_json_table_root(meta2, 'championship_results')
            championship_results = pd.concat([championship_results, _championship_results],sort=False)
    
    for k in championship_entries_all:
        championship_entries_all[k].reset_index(drop=True)
        if k in ['Driver', 'Co-Driver']:
            championship_entries_all[k] = championship_entries_all[k].rename(columns={'TyreManufacturer':'ManufacturerTyre'})
    
    return championship_lookup.reset_index(drop=True), \
            championship_results.reset_index(drop=True), \
            championship_entries_all, \
            championship_rounds.reset_index(drop=True), \
            championship_events.reset_index(drop=True)


#db utils
def cleardbtable(conn, table):
    ''' Clear the table whilst retaining the table definition '''
    c = conn.cursor()
    c.execute('DELETE FROM "{}"'.format(table))

def dbfy(conn, df, table, if_exists='upsert', index=False, clear=False, **kwargs):
    ''' Save a dataframe as a SQLite table.
        Clearing or replacing a table will first empty the table of entries but retain the structure. '''
    #print('{}: {}'.format(table,df.columns))
    if if_exists=='replace':
        clear=True
        if_exists='append'
    if clear: cleardbtable(conn, table)
        
    #Get columns  
    q="PRAGMA table_info({})".format(table)
    cols = pd.read_sql(q,conn)['name'].tolist()
    for c in df.columns:
        if c not in cols:
            print('Hmmm... column name `{}` appears in data but not {} table def?'.format(c,table ))
            df.drop(columns=[c], inplace=True)

    if if_exists=='upsert':
        DB = Database(conn)
        DB[table].upsert_all(df.to_dict(orient='records'))
    else:
        df.to_sql(table,conn,if_exists=if_exists,index=index)

def save_itinerary(meta, conn):
    itinerary_event, itinerary_legs, itinerary_sections, \
    itinerary_stages, itinerary_controls = getItinerary(meta)

    #dbfy(conn, itinerary_event, 'itinerary_event', if_exists='replace')
    dbfy(conn, itinerary_legs, 'itinerary_legs', if_exists='replace')
    dbfy(conn, itinerary_sections, 'itinerary_sections', if_exists='replace')
    dbfy(conn, itinerary_stages, 'itinerary_stages', if_exists='replace')
    dbfy(conn, itinerary_controls, 'itinerary_controls', if_exists='replace')


# Geo:
# What is df_rallydata?
# ALso TO DO: bring in a grabber for the live timing and SQLise that...

def get_kml_slug(meta):
    return meta['kmlfile'].unique().tolist()

def get_kml_slugs(df):
    return df['kmlfile'].unique().tolist()

def get_kml_file(kml_slug, outdirname='maps'):
    kmlurl = 'https://webappsdata.wrc.com/web/obc/kml/{}.xml'.format(kml_slug)
    r=requests.get(kmlurl)  
    with open("{}/{}.xml".format(outdirname,kml_slug), 'wb') as f:
        f.write(r.content)

def kml_to_json(kml_slug,indirname='maps', outdirname='geojson'):
    kml2geojson.main.convert('{}/{}.xml'.format(indirname,kml_slug),outdirname)

def kml_processor(o, indirname='maps',outdirname='geojson'):
    dirnames = [indirname, outdirname]
    for dirname in dirnames:
      if not os.path.exists(dirname):
        os.makedirs(dirname)
    if isinstance(o,dict):
      slugs = [o['kmlfile']]
    else:
      slugs = get_kml_slugs(o)
    for kml_slug in slugs:
        get_kml_file(kml_slug)
        kml_to_json(kml_slug,indirname,outdirname)


# TO DO: add to database
# get_kml_file('montecarlo_2019')
# kml_to_json('montecarlo_2019','maps/','maps/')
# We should also look at storing the data as geo object in geospatialite

def get_map_stages(gj):
    gff=[]
    for gf in gj['features']:
        #Handle SS 1/2 as SS1-2
        gf['properties']['name'] = gf['properties']['name'].replace('/','-').replace(' ','').strip()
        display(gf['properties']['name'])
        gff.append({'type': 'FeatureCollection',
     'features': [gf]})
    return gff



def _save_rally_base(meta, conn):
    display('Getting base info...')

    roster = getRoster(meta)
    dbfy(conn, roster, 'roster', if_exists='replace')

    startlists, startlist_classes = get_startlists(meta)
    dbfy(conn, startlists, 'startlists', if_exists='replace')
    dbfy(conn, startlist_classes, 'startlist_classes', if_exists='replace')

    save_itinerary(meta, conn)


def setup_db(dbname, meta, newdb=False):
    ''' Setup a database, if required, and return a connection. '''
    #In some situations, we may want a fresh start
    if os.path.isfile(dbname) and newdb:
        os.remove(dbname)

    if not os.path.isfile(dbname):
        #No db exists, so we need to create and populate one
        newdb = True

    #Open database connection
    conn = sqlite3.connect(dbname, timeout=10)

    if newdb:
        #Setup database tables
        c = conn.cursor()
        c.executescript(SETUP_Q)
        c.executescript(SETUP_VIEWS_Q)

        # Save season info
        display('NOT Grabbing season data tables for {}'.format(YEAR))
        #save_season_rounds(conn, year=YEAR)

        #Save championship info
        #If championship data aren't set, get the details into them...
        #Need a guard here... do a test on the db properly
        display('NOT Grabbing championship data tables for {}'.format(YEAR))
        #seed_championship(conn, year=YEAR)

        #Populate the database with event metadata
        display('Grabbing event metadata tables.')
        dbfy(conn, getEventMetadata(), 'event_metadata', if_exists='replace')

        #Get geo bits
        kml_processor(meta['event_meta'])

        #Save the entry list, initial itinerary etc
        _save_rally_base(meta, conn)

       

    return conn



#Grabbers
def save_rally(meta, conn, stage=None, stagetimes=True):
    ''' Save all tables associated with a particular rally.
        We can just get penalties and retirements by 
          setting stagetimes=False.
    '''

    #These need to be upserted
    display('Getting penalties...')
    penalties = get_penalties(meta)
    dbfy(conn, penalties, 'penalties')

    display('Getting retirements...')
    retirements = get_retirements(meta)
    dbfy(conn, retirements, 'retirements')

    if stagetimes:
      display('Getting stagewinners...')
      stagewinners = get_stagewinners(meta)
      dbfy(conn, stagewinners, 'stagewinners')

      display('Getting stage_overall...')
      stage_overall = get_overall(meta, stage)
      dbfy(conn, stage_overall, 'stage_overall')

      display('Getting split_times...')
      split_times = get_splitTimes(meta, stage)
      dbfy(conn, split_times, 'split_times')
      
      display('Getting stage_times_stage...')
      stage_times_stage = get_stage_times_stage(meta, stage)
      dbfy(conn, stage_times_stage, 'stage_times_stage')
      
      display('Getting stage_times_overall...')
      stage_times_overall = get_stage_times_overall(meta, stage)
      dbfy(conn, stage_times_overall, 'stage_times_overall')


def save_championship(conn, year=YEAR):
    ''' Save all championship tables for a particular year. '''
    championship_lookup, championship_results, _championship_entries_all, \
        championship_rounds, championship_events = championship_tables(year=year)
        
    championship_entries_drivers = _championship_entries_all['Drivers']
    championship_entries_codrivers = _championship_entries_all['Co-Drivers']
    championship_entries_manufacturers = _championship_entries_all['Manufacturers']
    #championship_entries_nations = _championship_entries_all['Nations']
    
    dbfy(conn, championship_lookup, 'championship_lookup')#, if_exists='replace')
    dbfy(conn, championship_results, 'championship_results')#, if_exists='replace')
    dbfy(conn, championship_entries_drivers, 'championship_entries_drivers')#,if_exists='replace')
    dbfy(conn, championship_entries_codrivers, 'championship_entries_codrivers')#, if_exists='replace')
    dbfy(conn, championship_entries_manufacturers, 'championship_entries_manufacturers')#, if_exists='replace')
    # TO DO - Getting an error if we try to upsert championship_rounds becuase of col named 'order':
    #https://github.com/simonw/sqlite-utils/issues/10
    dbfy(conn, championship_rounds, 'championship_rounds', if_exists='replace')
    # TO DO - uosert error if column name contains a .
    dbfy(conn, championship_events, 'championship_events', if_exists='replace')

def seed_championship(conn, year=YEAR):
  ''' Get championship data if not already in db. '''
  #TO DO - only do this if required?
  dbfy(conn, pd.DataFrame(get_seasons()),"season")
  save_championship(conn, year=YEAR)


def get(rally, dbname='wrc19_test1.db', year=YEAR, 
        running=False, stage=None, defaultstages='run', championship=False):
    ''' Get specified stages. If a stage is explicitly identified, just get that stage.
        Else by default get all run stages (defaultstages='run').
        Force download of all stages with: defaultstages='all'
        defaultstages: all | run '''


    #Should we go wholesale and just use event metadata?

    set_rallyId(rally, year)

    stubs['url_base'] = stubs['url_base_pattern'].format(SASEVENTID=getEventIDs(year)[rally])

    #print('Meta is:',meta)
    
    #We then bring meta dict pointer into local functional scope? Why??
    #conn = sqlite3.connect(dbname)
    conn = setup_db(dbname, meta)

    #This is duplicated if we set up a new db...
    #The save_itinerary step also calls the itinerary and updates meta
    save_itinerary(meta, conn)

    #stage = stage if isinstance(stage,list) else [stage]
    if stage:
      stage = stage if isinstance(stage,list) else [stage]
    else:
      stage = list(meta['_stages'].keys())

    #We can ignore stages that are ToRun - we can ignore this if we want to forceall
    meta['torun'] = pd.read_sql('SELECT code FROM itinerary_stages WHERE status="ToRun"',conn)['code'].to_list()
  
    #We can limit ourselves to just grabbing running stages
    #This info is already in meta['_stages']
    if running:
      running_stages = pd.read_sql('SELECT code FROM itinerary_stages WHERE status="Running"',conn)['code'].to_list()
      stage = [s for s in stage if s in running_stages ]
      print('Getting data for any running stages:', stage)

    #Omit stages that are torun - should we do this later?
    #All stageIds are in meta['_stages]
    print('Getting {} stages {} for {}; still to run: {}'.format(defaultstages, ', '.join(stage), rally, meta['torun']))
    if not stage and defaultstages=='run':
      stage = [s for s in stage if s not in meta['torun']]
    elif defaultstages=='all':
      #This plays on the default behaviour of save_rally()
      stage = None
      #stage = [s for s in meta['_stages']]
    
    save_rally(meta, conn, stage=stage, stagetimes= (defaultstages != 'none') )

    #Do we need to save championship here?
    if championship:
      save_championship(conn, year=year)

def get_championship(dbname='wrc19_test1.db', year=YEAR):
    #Should we really use or pass in a conn in to a db
    # that we know is properly configured?
    conn = setup_db(dbname, meta)

    display('Grabbing championship data tables for {}'.format(year))
    save_championship(conn, year=year)

#More initialisation
meta['rallies'][YEAR]= getEventIDs(YEAR)

# Checks...
# q="SELECT name FROM sqlite_master WHERE type = 'table';"
# pd.read_sql(q,conn)

# This needs to be split up...


# TO DO - allow setting of things like dbname as an env var

def showrallies(year=None):
  ''' Display rallies for current or specified year. '''
  if year is None:
    year = datetime.datetime.now().year
  if year not in meta['rallies']:
    meta['rallies'][year]= getEventIDs(year)
  availableRallies = '\nAvailable rallies for {} are: {}\n'.format(year, ', '.join([k for k in meta['rallies'][year]]))
  display(availableRallies)

@click.command()
@click.option('--year',default=datetime.datetime.now().year,help='Year results are required for (defaults to current year)')
def cli_showrallies(year):
    ''' Show available rallies. '''   
    showrallies(year)

@click.command()
def cli_metadata(year, name, stages):
    ''' Refresh WRC metadata in database. '''
    conn = setup_db(dbname)

    display('\nUpdating metadata...')
    dbfy(conn, getEventMetadata(), 'event_metadata', if_exists='replace')

@click.command()
@click.option('--year',default=datetime.datetime.now().year,help='Year results are required for (defaults to current year)')
@click.option('--dbname', default='wrc_timing.db',  help='SQLite database name')
@click.option('--running', is_flag=True, help='Only grab stages that are running')
@click.option('--championship', is_flag=True, help='Grab championship tables too')
@click.option('--default-stages', default=('run'), type=click.Choice(["all", "run", "none"]),  help='If no stages specified, grab what we can. ')
@click.argument('name')
@click.argument('stages', nargs=-1)
def cli_get(year, dbname, running, championship, default_stages, name, stages):
  ''' Get stages for a given rally. '''
  if not name:
    display('Which rally? To see available rallies, run: wrc_rallies')
  else:
    try:
      display('\nGetting data for stages of {} {}: {}'.format(name, year, defaultstages))
      get(name, dbname=dbname, year=year, running=running, stage=stages, defaultstages=default_stages, championship=championship )
    except:
      display('\nHmm... something went wrong in get...\nCheck rally name by running: wrc_rallies --year {}'.format(year))
      # TO DO - also check stages? Can we get a stage list?

@click.command()
@click.option('--year',default=datetime.datetime.now().year,help='Year results are required for (defaults to current year)')
@click.option('--dbname', default='wrc_timing.db',  help='SQLite database name')
@click.argument('name')
def cli_fullRun(year, dbname, name):
  ''' Get all data for all rallies in a year. '''

  if not name:
        display('Which rally? To see available rallies, run: wrc_rallies')
  else:
      try:
          for name in listRallies():
              display('Trying to get data for {}, {}'.format(name, year))
              get(name, dbname=dbname, year=year )
      except:
          display('Hmm... something went wrong...\nCheck rally name by running: wrc_rallies {}'.format(year))
          # TO DO - also check stages? Can we get a stage list?

@click.command()
@click.option('--year',default=datetime.datetime.now().year,help='Year results are required for (defaults to current year)')
@click.option('--dbname', default='wrc_timing.db',  help='SQLite database name')
@click.argument('command')
def cli_get_championship(year, dbname, command):
    ''' Get championship details. '''
    if command=='fetch':
        get_championship(dbname=dbname, year=year )
