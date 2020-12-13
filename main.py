from slugify import slugify
import boto3, csv, time, urllib3, sys

# disable ssl warning(not reccomended)
urllib3.disable_warnings()

# Stop credentials from being used:
from botocore import UNSIGNED
from botocore.client import Config

recurse = True  # -r, Recurse, continue getting keys if returned a truncated list(will fetch EVERY key in bucket)
verbose = True  # -v, --verbose, lists all keynames as they are being fetched, shows progress every 1000 keys
checkACL = True  # --acl, check wether each key is public or private
numKeys = [0, 0]  # totalkeys,public keys
startTime = time.time()  # time we started the script
runTime = None  # run time, updates in script
estimateTimes = []  # for getEstimate, saves request times from to work out average response time
averageEstTime = None  # average of estimateTimes
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# print current progress to the terminal
def printProgress():
    if checkACL:
        logString = '\r  Progress: keys: ' + str(numKeys[0]) + ', public: ' + str(
            numKeys[1]) + ', current runTime: ' + runTime + 's'
    else:
        logString = '\r  Progress: keys: ' + str(numKeys[0]) + ', current runTime: ' + str(runTime) + 's'
    sys.stdout.write(logString)  # print logstring
    sys.stdout.flush()  # clear print line

# check if a key inside a bucket is public
def isObjPublic(_bucket, _key):
    try:
        # try to read properties object, works if access is public
        obj = s3.head_object(Bucket=_bucket, Key=_key)
        return (True, 'public')
    except Exception as e:
        # object read failed
        error = str(e)
        # shorten error message
        if 'Forbidden' in error:
            error = 'private'
        elif 'Not Found' in error:
            error = 'not found'
        return (False, error)
    # e returns either:
    # 'An error occurred (404) when calling the HeadObject operation: Not Found'
    # 'An error occurred (403) when calling the HeadObject operation: Forbidden'

# loop through bucket, get all keys, save to csv
def getKeys(_bucket,_marker=None):

    filename = unicode(_bucket + ('_' + _marker if _marker else ''), 'utf-8').encode('utf-8')

    # print info
    print '\x1b[0;33;49mFetching keys from: ' + filename.encode('utf-8')

    lab_session = boto3.Session()
    c = lab_session.client('s3')  # this client is only for exception catching

    try:
        if _marker:
            objects = s3.list_objects(Bucket=_bucket, Marker=_marker)
        else:
            objects = s3.list_objects(Bucket=_bucket)
    except c.exceptions.NoSuchBucket as e:
        return
    except Exception as e:
        return

    with open('./results/' + slugify(filename) + '.csv', 'w') as fh:
        fieldnames = ['key', 'size', 'last_mod', 'access']
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for key in objects['Contents']:
            global numKeys, runTime
            numKeys[0] += 1  # update number of keys
            keyStr = key['Key'].encode(
                'utf-8')  # get key and make sure it is encoded to utf-8, otherwise throws errors with some lang characters
            size = key['Size']  # get file size in bytes
            lastMod = str(key['LastModified'])  # get date of last modified
            access = 'unknown'  # wether object is public/private
            logString = keyStr  # start making string to print to terminal, only if -v|--verbose was enabled
            t0 = time.time()  # time logging
            runTime = "{0:.2f}".format(t0 - startTime)  # update long script has been running, total
            # if check(if key public) flag = True
            if checkACL:
                ispublic = isObjPublic(_bucket, keyStr)  # check if object is public
                access = ispublic[1]  # get returned string 'public'|'private'
                logString += ': ' + access  # append logString with acesss. Example: 'Files/images/1.jpg:public'
                if ispublic[0]: numKeys[1] += 1  # if this object IS public, increase total public keys tally
            # write info to file
            writer.writerow({'key': keyStr, 'size': size, 'last_mod': lastMod, 'access': access})
            if verbose:
                print(logString)  # if -v|--verbose, print logstring
            else:  # else
                printProgress()  # print current progress(continusly updates one line)
        # if user does not want truncated list but objects IS truncated recursivly continue from last key
        if recurse and objects['IsTruncated']:  # note:this fires once every thousand keys
            lastKey = objects['Contents'][-1]['Key']  # get the last key
            if verbose:  # if -v|--verbose
                print(
                            '\x1b[1;32;49mNext Marker:\x1b[0;37;49m ' + lastKey)  # print info. Example: 'Next Marker: Files/images/1.jpg'
                print('\x1b[1;32;49mRun time:\x1b[0;37;49m ' + runTime + 's, \x1b[1;32;49mTotal Keys:\x1b[0;37;49m ' + str(
                    numKeys[0]))  # print info
            getKeys(bucket, lastKey)  # recursivly continue fetching keys, until list is no longer truncated

with open("companies_oneline.csv", "r") as filestream:
    for line in filestream:
        currentline = line.split(",")
        for bucket in currentline:
            getKeys(bucket)  # get keys starting at beginning of bucket, and write to .csv file

if __name__ == '__main__':
    open()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
