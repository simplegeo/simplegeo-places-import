#!/usr/bin/python
import sys
import csv
import ngram
import simplegeo.places
import traceback
import time

# the incoming CSV must have at least id, lat, lon, and name. Will also pick up any other columns as properties.
# location, owner, deleted, handle, id are reserved properties - we'll lose anything you input with those names
# additionally will be more lenient on name match if it can match on address (and phone)

def main():
    infile = sys.argv[1]
    outfile = infile.replace(".csv","+SGh.csv")

    SGkey = ''
    SGsecret = ''

    try:
        client = simplegeo.places.Client(SGkey, SGsecret)

        # don't read in as a dict because we want to put all the original columns back 
        newplaces = open(infile ,mode='r')
        preader = csv.reader(newplaces)

        linenum = 0
        # get the header row, check that it has our required fields
        headers = preader.next()
        headerset = set(headers)
        if 'id' not in headerset or 'name' not in headerset or 'lat' not in headerset or 'lon' not in headerset:
            sys.exit('id, name, lat, and lon are all required input columns')

        merges = open(outfile, mode='w')
        mwriter = csv.writer(merges) 
        head = ['source_id','SG_handle','SG name','SG address','SG phone','already in SGP']
        head.extend(headers)
        mwriter.writerow(head)

        numfound = 0

        for row in preader:
            linenum += 1

            line = dict(zip(headers,row))
            # use the biggest word in the name to query on. Too easy to not find place in search if area is dense otherwise

            words = line['name'].lower().split()
            wordlens = [len(a) for a in words]
            biglen = max(wordlens)
            bigword = words[wordlens.index(biglen)]
           
            # try to search a few times - sometimes there's a timeout, but usually it gets better in a few seconds 
            for i in range(3):
                try: 
                    # we should agree within 100m on where the feature is
                    results = client.search(float(line['lat']),float(line['lon']),query=bigword, radius=.1)
                    break
                except Exception:
                    traceback.print_exc()
                    apiheaders = client.get_most_recent_http_headers()
                    print 'most recent headers: %s' % apiheaders

                    stime = i*10
                    time.sleep(stime)                        
                    if i == 3:
                        sys.exit(1)

            found = 0
            if len(results) > 0:  
                trisims = [ngram.NGram.compare(line['name'].lower(),feature.properties['name'].lower(),N=3) for feature in results]
                topscore = max(trisims)
                topmatch = results[trisims.index(topscore)]

                address = None
                phone = None
            
                if 'address' in topmatch.properties:
                    address = topmatch.properties['address']
                if 'phone' in topmatch.properties:
                    phone = topmatch.properties['phone']

                if topscore > .65:
                    # match if names are very close
                    found = 1
                elif 'address' in line and address != None:
                    if line['address'] == address and topscore > .5:
                        # match if names are close and addresses are equal
                        found = 1
                    if 'phone' in line and phone != None and ngram.NGram.compare(line['address'].lower(),address.lower(),N=3) > .6and line['phone'] == phone.replace('+1','').replace(' ','') and topscore > .5:
                        # match if names are close, addresses are close, and phones are equal
                        found = 1 

                if found == 1:
                    numfound +=1
                    outrow = [line['id'],topmatch.id,topmatch.properties['name'],address,phone,1]
                    outrow.extend(row)
                    mwriter.writerow(outrow)
 

            # if we didn't find a match, then create a feature and pass it to add_feature
            if found == 0:
                props = dict(line)
                props.pop('lat')
                props.pop('lon')
                props.pop('id')
                # id is reserved for SGP internal use, record_id is the correct name for id from source system
                props['record_id'] = line['id']

                f = simplegeo.places.Feature((float(line['lat']),float(line['lon'])),properties=props)
                newSGh = client.add_feature(f)

                outrow = [line['id'], newSGh,'','','', 0]
                outrow.extend(row)
                mwriter.writerow(outrow)

        print "Processed %s records" % linenum
        print "Found %s records in SimpleGeo Places" % numfound
        print "Added %s records to SimpleGeo Places" % (linenum - numfound)
            
    except Exception:
        # Get the most recent exception
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        # Exit the script and print an error telling what happened
        traceback.print_tb(exceptionTraceback)
        sys.exit("%s ->%s : %s" % (exceptionType, exceptionValue, exceptionTraceback))

 
if __name__ == "__main__":
    sys.exit(main())

