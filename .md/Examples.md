---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.1'
      jupytext_version: 1.2.4
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# Examples

Examples of how to work with the package in a script.

```python
#Set the notebook up to support live development in the module file
%load_ext autoreload
%autoreload 2
```

```python
import wrc_livetiming as wt
```

```python
wt.showrallies()
```

```python
wt.meta

```

```python
!rm wales-test.db
```

```python
wt.get('spain','spain-test.db')
```

```python
#!rm sardegna.db
wt.get('sardegna', 'sardegna.db')
```

```python
wt.get('germany', 'germanTest.db')

```

```python
!ls -al
```

```python
stubs=wt.stubs
meta=wt.meta
itinerary_json=requests.get( stubs['url_base'].format(stub=stubs['itinerary'].format(**meta) ) ).json()

itinerary_json
```

```python
json_normalize(itinerary_json['itineraryLegs'])#.drop('itineraryLegs', axis=1)
```

```python
!rm finland19a.db
```

```python
wt.showrallies()
```

```python
!ls -al
```

```python
import sqlite3

dbname2='wales-test.db'
conn = sqlite3.connect(dbname2)
wt.dbfy(conn, df,"season", if_exists='replace')
```

```python
type(df)
```

```python
q="SELECT * FROM season LIMIT 1;"
pd.read_sql(q,conn).columns
```

```python
from pandas.io.json import json_normalize
wt.get_season_rounds(1).dtypes
```

```python
wt.get_seasons()
```

```python
wt.get_seasons()

```

```python
wt.get_season_rounds(2019)
```

```python
wt.get_season_rounds(2019)
```

```python
wt.get_seasonId(2019)
```

```python
wt.get_seasonId(2019)
```

```python
s='''
'event.categories', 'event.clerkOfTheCourse', 'event.country.countryId',
       'event.country.iso2', 'event.country.iso3', 'event.country.name',
       'event.countryId', 'event.eventId', 'event.finishDate',
       'event.location', 'event.mode', 'event.name', 'event.organiserUrl',
       'event.slug', 'event.startDate', 'event.stewards', 'event.surfaces',
       'event.templateFilename', 'event.timeZoneId', 'event.timeZoneName',
       'event.timeZoneOffset', 'event.trackingEventId', 'eventId', 'order',
       'seasonId'
       '''
s.replace("'",'"').replace('\n','')#.replace(',',',\n')
```

```python
wt.meta
```

```python
wt.YEAR
```

```python
wt.getRallyIDs2()
```

```python
wt.getRallyIDs()
```

```python
wt.meta
```

```python
# har
# !pip3 install har-extractor
# Command line: har-extractor test.har 
# or from py: https://browsermob-proxy-py.readthedocs.io/en/stable/

# Firefox seems to let you filter network items and just export filtered ones to har
# Then open har file as a json file and filter on the url to identify the request
# Could probably do this in tandem with selenium - use selenium to open browser 
# and walk through pages, then manually save the filtered har
```

```python
#!pip3 install selenium-wire
from seleniumwire import webdriver  # Import from seleniumwire

# Create a new instance of the Firefox driver
driver = webdriver.Firefox()

# Go to a WRC live timing page
driver.get('https://www.wrc.com/en/wrc/livetiming/page/4175----.html')

```

```python
# Access requests via the `requests` attribute
for request in driver.requests:
    if request.response:
        if 'sasCache' in request.path:
            print(
                request.path,
                request.response.status_code,
                request.response.headers,
                request.body,
                '\n'
            )
```

```python
j = [r for r in driver.requests if r.response and 'https://www.wrc.com/service/sasCacheApi.php' in r.path]
j

```

```python
dir(j[0])
```

```python
dir(j[1].response)
```

```python
#!pip3 install mitmproxy
#command line: mitmdump -w test1
#close with: ?? ctrl-c ?



#Running from py? https://github.com/mitmproxy/mitmproxy/issues/3306
#Then maybe filter w/ a riff on s/thing like:
# https://github.com/mitmproxy/mitmproxy/blob/master/examples/simple/io_write_dumpfile.py ?
# this is maybe more useful?
# https://ironhackers.es/en/tutoriales/man-in-the-middle-modificando-respuestas-al-vuelo-con-mitmproxy/

#filter with eg: mitmdump -nr test1 -w test4 "~u .*sasCacheApi.*"



from selenium import webdriver

PROXY = "localhost:8080" # IP:PORT or HOST:PORT

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--proxy-server=%s' % PROXY)
chrome_options.add_argument("--headless") 

chrome = webdriver.Chrome(chrome_options=chrome_options)
chrome.get("https://www.wrc.com/en/wrc/livetiming/page/4175----.html")


chrome.close()
```

```python
chrome.close()
```

```script magic_args="bash --bg"
mitmdump -w test5 "~u .*sasCacheApi.*"
```

```python
#Process numbers 
! ps -e | grep 'mitmdump' | awk '{print $1 " " $4}'
```

```python
#Or to kill eg
!kill $(ps -e | grep 'mitmdump' | awk '{print $1}' )
```

```python
! ps -Al | grep mitm
```

```python
#! kill 94729
```

```python
#Command line for web viewer: mitmweb
from mitmproxy import io
from mitmproxy.exceptions import FlowReadException
import pprint
import sys


with open('/Users/tonyhirst/Downloads/test4', "rb") as logfile:
    freader = io.FlowReader(logfile)
    pp = pprint.PrettyPrinter(indent=4)
    try:
        for f in freader.stream():
            print(f)
            print(f.request.host)
            pp.pprint(f.get_state())
            print("")
    except FlowReadException as e:
        print("Flow file corrupted: {}".format(e))
```

```python
from mitmproxy import io
from mitmproxy.net.http.http1.assemble import assemble_request

def response(flow):
    print(assemble_request(flow.request).decode('utf-8'))
```

```python
with open('/Users/tonyhirst/Downloads/test4', "rb") as logfile:
    freader = io.FlowReader(logfile)
    for f in freader.stream():
        response(f)
```

```python
f.get_state().keys()
```

```python
f.get_state()['response']
```

```python
f.get_state()['request']['path'].decode().split('=')[1].replace('%2F','_').replace('%3F','_').replace('%3D','_')
```

```python
f.get_state()['response']['content']
```

```python
import gzip
text = gzip.decompress(f.get_state()['response']['content'])
text
```

```
#need asyncio ???
# eg below doesn't work?
#https://discourse.mitmproxy.org/t/solved-mitmproxy-in-separated-python-thread/1050


#https://github.com/mitmproxy/mitmproxy/issues/3306#issuecomment-436325230
from mitmproxy import proxy, options
from mitmproxy.tools.dump import DumpMaster

class AddHeader:
    def request(self, flow):
        if flow.request.pretty_host == "example.org":
            flow.request.host = "mitmproxy.org"


#myaddon = AddHeader()
opts = options.Options(listen_host='0.0.0.0', listen_port=8080,# mode='transparent',
                       #confdir='/home/user/.mitmproxy'
                      )
pconf = proxy.config.ProxyConfig(opts)
m = DumpMaster(opts)
m.server = proxy.server.ProxyServer(pconf)
#m.addons.add(myaddon)

#    try:
#        m.run()
#    except KeyboardInterrupt:
#        m.shutdown()

m.run()

```

```python
import gzip
def response2(flow):
    fn = flow.get_state()['request']['path'].decode()
    fn = fn.split('=')[1].replace('%2F','_').replace('%3F','_').replace('%3D','_')
    with open('{}.json'.format(fn),'wb') as outfile:
        outfile.write( gzip.decompress(flow.get_state()['response']['content']) )
    
with open('/Users/tonyhirst/Downloads/test4', "rb") as logfile:
    freader = io.FlowReader(logfile)
    for f in freader.stream():
        response2(f)
```

```python
!cat events_87_rallies_103_entries.json
```

```python

```
