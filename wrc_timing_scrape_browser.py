# # WRC Live Timing - Browser Based Scraper
#
#
# Calling the API directly under script conditions seems to occasionally result in an empty response, although calling the WRC live timing pages via a web page request always seems to return a complete payload.
#
# This scraper uses browser automation to navigate the WRC live timing site, capturing JSON responses via the selenium HAR archive filter.

# ## Load the WRC Live Timing Homepage



# For each stage, for each tab:
#
# OVERALL, SPLIT TIMES, STAGE TIMES
#
# click the tab and grab the JSON.
#
# The following tabs only need grabbing once:
#
# STAGEWINNERS, ITINERARY, STARTLISTS, PENALTIES, RETIREMENTS
#
#
# Championship results require each tab:
#
# DRIVER, CO-DRIVER, MANUFACTURERS
#
# for each class in drop down.
#
#
# There is also a calendar page:
#
# https://www.wrc.com/en/wrc/calendar/calendar/page/671-29772-16--.html
