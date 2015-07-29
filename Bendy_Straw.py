import argparse
import json
import pprint
import sys
import urllib
import urllib2
import oauth2
import time
import random
import requests
import csv


from bs4 import BeautifulSoup


API_HOST = 'api.yelp.com'
DEFAULT_TERM = 'Food'
DEFAULT_LOCATION = 'Los Angeles, CA 90024'
SEARCH_LIMIT = 20
SEARCH_SORT = 1
SEARCH_PATH = '/v2/search/'
BUSINESS_PATH = '/v2/business/'
MAX_QUERIES = 10000
queryCount = 0

# OAuth credentials
CONSUMER_KEY = 'MjxfYhb66YWLqJYMVxXw9A'
CONSUMER_SECRET = 'iLbP__sxL2sFXp6yOrGD7asaFM8'
TOKEN = 'WW7q8N80VJog2qq_FFPQpsKwxxKwhcAr'
TOKEN_SECRET = 'CYOhmZp1v6rqZ2UYQ42UdWlvzk8'


def request(host, path, url_params=None):

    global queryCount
    if queryCount > MAX_QUERIES:
        sys.exit('Exceeded maximum daily allowable calls to API')

    queryCount = queryCount + 1

    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(host, urllib.quote(path.encode('utf8')))

    consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': CONSUMER_KEY
        }
    )
    token = oauth2.Token(TOKEN, TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()

    try:
        conn = urllib2.urlopen(signed_url, None)
    except:
        time.sleep(30*random.random())
        return request(host, path, url_params)

    try:
        response = json.loads(conn.read())
    finally:
        conn.close()

    return response


def search(term, location, offset):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'offset': offset.replace(' ', '+'),
        'sort': SEARCH_SORT,
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, url_params=url_params)


def get_business(business_id):
    business_path = BUSINESS_PATH + business_id
    return request(API_HOST, business_path)


def save_output(scrape_output, api_output):
    
    outFile = open(outFileName, 'a')
    outFile.write('\n')
    default = ''

    locationMacro = api_output.get('location')
    #if locationMacro is not None:
    outStringFirst = unicode(api_output.get('id', default)) + ', ' + unicode(api_output.get('name', default)) + ', ' + unicode(api_output.get('is_claimed', default)) + ', ' + unicode(api_output.get('phone', default)) + ', ' + unicode(api_output.get('review_count', default)) + ', ' + unicode(api_output.get('rating', default)) + ', ' + unicode(locationMacro.get('city', default)) + ', ' + unicode(locationMacro.get('state_code', default)) + ', ' + unicode(locationMacro.get('postal_code',default)) + ', '
    #else:
    #    outStringFirst = unicode(api_output.get('id')) + ', ' + unicode(api_output.get('name')) + ', ' + unicode(api_output.get('is_claimed')) + ', ' + unicode(api_output.get('phone')) + ', ' + unicode(api_output.get('review_count')) + ', ' + unicode(api_output.get('rating')) + ', ' + ', ' + ', ' + ', '

    outStringSecond = ''
    for i in range(len(attribute_list)):
        outStringSecond = outStringSecond + unicode(scrape_output[attribute_list[i]]).replace(',','+') + ', '

    outStringLast = ''
    categoryList = api_output.get('categories')
    if categoryList is not None:
        for j in range(len(categoryList)):
            tempString = unicode(categoryList[j]).encode('ascii')
            outStringLast = outStringLast + unicode(tempString) + ', '
    
    finalWriteString = outStringFirst + outStringSecond + outStringLast
    outFile.write(finalWriteString)
    outFile.close()


def scrape_page(business_id):

    attribute_dict = {
        'Price range' : '',
        'Takes Reservations' : '',
        'Delivery' : '',
        'Take-out' : '',
        'Accepts Credit Cards' : '',
        'Good For' : '',
        'Parking' : '',
        'Bike Parking' : '',
        'Good for Kids' : '',
        'Good for Groups' : '',
        'Attire' : '',
        'Ambience' : '',
        'Noise Level' : '',
        'Alcohol' : '',
        'Outdoor Seating' : '',
        'Wi-Fi' : '',
        'Has TV' : '',
        'Waiter Service' : '',
        'Caters' : ''}

    global attribute_list
    attribute_list = ('Price range', 'Takes Reservations', 'Delivery', 'Take-out', 'Accepts Credit Cards', 'Good For', 'Parking', 'Bike Parking', 'Good for Kids', 'Good for Groups', 'Attire', 'Ambience', 'Noise Level', 'Alcohol', 'Outdoor Seating', 'Wi-Fi', 'Has TV', 'Waiter Service', 'Caters')

    try:
        proxyList = ('108.62.70.204:3128',
                     '173.234.232.42:3128',
                     '50.31.10.9:3128',
                     '50.31.10.165:3128',
                     '108.62.70.67:3128',
                     '50.31.10.123:3128',
                     '173.234.232.25:3128',
                     '50.31.10.101:3128',
                     '108.62.70.4:3128',
                     '173.234.232.26:3128',
                     '50.31.10.116:3128',
                     '108.62.70.25:3128',
                     '173.234.232.113:3128',
                     '108.62.70.75:3128',
                     '50.2.15.173:3128',
                     '173.234.232.116:3128',
                     '50.2.15.84:3128',
                     '50.31.10.218:3128',
                     '173.234.232.70:3128',
                     '173.234.232.136:3128',
                     '173.234.232.216:3128',
                     '108.62.70.51:3128',
                     '173.234.232.132:3128',
                     '50.31.10.34:3128')

        randProxyFinder = random.randint(0,23)

        proxy = urllib2.ProxyHandler({'http': proxyList[randProxyFinder]})
        print 'Trying to proxy through IP address ' + unicode(proxyList[randProxyFinder])

        opener = urllib2.build_opener(proxy)
        urllib2.install_opener(opener)

        address = 'http://www.yelp.com/biz/' + business_id
        html = urllib2.urlopen(address, timeout = 30).read()
        print 'Connected to proxy ' + unicode(proxyList[randProxyFinder])

    except:
        time.sleep(10*random.random())
        return scrape_page(business_id)
        #return attribute_dict

    #print '\nScraped attributes for: ' + business_id

    soup = BeautifulSoup(html, 'html.parser')
    attribute = soup.find_all("dt", class_="attribute-key")
    for i in range(len(attribute)):

        field = attribute[i].next_element
        fieldName = unicode(field.strip())
        fieldName = fieldName.replace('\n','')

        i2 = field.next_element
        i3 = i2.next_element
        i4 = i3.next_element
        fieldVal = unicode(i4.strip())
        fieldVal = fieldVal.replace('\n','')
        
        if fieldName in attribute_list:
            attribute_dict[fieldName] = fieldVal
            #print unicode(fieldName) + ': ' + unicode(fieldVal)
    
    print 'Scraping completed for ' + business_id
    time.sleep(15*random.random())
    return attribute_dict


def query_api(term, location, offset):
    response = search(term, location, offset)
    businesses = response.get('businesses')

    if not businesses:
        print u'No businesses for {0} in {1} found.'.format(term, location)
        return
    
    for i in range(len(businesses)):
        business_id = businesses[i]['id']

        foundBool = 0

        dictionaryFile = open('OutletDictionary.csv', 'a+')
        reader = csv.DictReader(dictionaryFile)
        for row in reader:
            if (row['ID']).encode('utf-8') == (business_id).encode('utf-8'):
                print 'Data for ' + business_id + ' already exists.\n'
                foundBool = 1
                break

        if foundBool == 0:
            if all(ord(c) < 128 for c in business_id):
                scrape_response = scrape_page(business_id)
                    
                api_response = get_business(business_id)
                print 'API data received for ' + business_id

                save_output(scrape_response, api_response)
                print 'Data for ' + business_id + ' written to file. Continuing... \n'

                fieldNames = ['ID']
                writer = csv.writer(dictionaryFile, dialect = 'excel', lineterminator = '\n')
                fieldVal = [unicode(business_id)]
                writer.writerow(fieldVal)

        dictionaryFile.close()
    
    return response


def main():

    global outFileName
    city = 'Los Angeles, CA '
    #90003
    zipCodes = (90004, 90005, 90006, 90007, 90010, 90011, 90012, 90013, 90014, 90015, 90017, 90018, 90019, 90001, 90002, 90020, 90021, 90024, 90025, 90026, 90027, 90028, 90029, 90031, 90033, 90034, 90036, 90037, 90038, 90041, 90042, 90043, 90044, 90046, 90047, 90048, 90049, 90057, 90061, 90062, 90064, 90065, 90067, 90068, 90069, 90071, 90077, 90079, 90089, 90090, 90094, 90095, 90272, 90275, 90291, 90293, 90402, 90405, 90502, 91040, 91042, 91105, 91303, 91304, 91306, 91311, 91316, 91324, 91325, 91326, 91330, 91331, 91335, 91340, 91342, 91343, 91344, 91345, 91352, 91356, 91364, 91367, 91371, 91401, 91402, 91403, 91405, 91406, 91411, 91423, 91436, 91601, 91602, 91604, 91606, 91607, 91608)
    searchTerms = ('indian food', 'thai food', 'mexican food', 'dessert', 'japanese food', 'korean', 'italian food', 'restaurant', 'chinese food', 'pizza', 'sandwich', 'coffee', 'bar', 'fine dining')
    pages = 1

    for zipCodeIndex in range(len(zipCodes)):
        for searchTermIndex in range(len(searchTerms)):
            for pageIndex in range(pages):
                try:
                        outFileName = unicode(zipCodes[zipCodeIndex]) + '.csv'
                        outFile = open(outFileName, 'a')
                        outFile.write('ID,Name,Claimed?,Phone,Count,Rating,City,State,Zip-code,Price,Reservations,Delivery,Take-out,Credit cards,Good for,Parking,Bike,Kids,Groups,Attire,Ambience,Noise,Alcohol,Outdoor,WiFi,TV,Waiter,Caters,Tag 1,Tag 2,Tag 3,Tag 4,Tag 5,Tag 6,Tag 7,Tag 8,Tag 9\n')
                        outFile.close()

                        print 'Now checking ' + unicode(searchTerms[searchTermIndex]) + ' in ' + unicode(zipCodes[zipCodeIndex])
                        response = query_api(searchTerms[searchTermIndex], city + unicode(zipCodes[zipCodeIndex]), unicode(pageIndex))
                except urllib2.HTTPError as error:
                    sys.exit('Encountered HTTP error {0}. Abort program.'.format(error.code))    
                time.sleep(60*random.random())

if __name__ == '__main__':
    main()