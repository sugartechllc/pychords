#! /usr/local/bin/python3
# pylint: disable=C0103
# pylint: disable=C0325

import json
import time
import sys
import requests
import _thread

"""
Send CHORDS data structures to a CHORDS instance.

The JSON configuration file must contain at least:
{
      "chords_host": "chords_host.com",
      "skey": "key"
}
It may also contain both of the following, if using the 
CHORDS api key authentication method:
{
      "api_email": "registered chords user email",
      "api_key": "registered chords user api key"
}

It is fine to include all of the configuration
needed by other modules (e.g. FromWxflow and WxflowDecode).

"""

uri_queue = []
uri_queue_lock = _thread.allocate_lock()

def sendRequests(arg):
    """
    Check the queu once per second, and send any waiting URI requests.
    """

    while True:
        # Get a uri from the queue
        uri_queue_lock.acquire()
        if len(uri_queue):
            uri = uri_queue.pop(0)
        else:
            uri = None
        uri_queue_lock.release()

        if uri:
            uri_sent = False
            while not uri_sent:
                try:
                    # Transmit the request
                    response = requests.get(uri)
                    response.close()
                    uri_sent = True
                    print("Sent:", uri)

                except Exception as ex:
                    print (
                        "Error in ToChords.sendRequests:",
                        str(ex.__class__.__name__), str(ex), ex.args)
                    # If request is sent too often, we may get a MaxRetryError
                    time.sleep(2)

        else:
            # Empty queue, sleep
            time.sleep(1)

def startSender():
    """
    Start the sending thread.
    """
    _thread.start_new_thread(sendRequests, (None,))

def buildURI(chords_host, uri_params):
    """
    host: The CHORDS host.
    uri_params: Items which will be used to build url_create.
    If api_key AND api_email are provided, the CHORDS api key authentication will be used.
    Otherwise, skey must be provided and the deprecated key method will be used.
    {
      "inst_id": "1",
      "skey": "123456",
      "api_email": "registered chords user email", 
      "api_key": "registered chords user api key"
      "test": False,
      "vars": {
        "at": 1511456154,
        "lcount": 0,
        "ldist": 0,
        "pres": 770.0,
        "rh": 33,
        "tdry": 13.43,
        "vbat": 3.46
      }
    }
    """

    chords_uri = "http://" + chords_host + "/measurements/url_create?"
    chords_uri = chords_uri + "instrument_id=" + uri_params["inst_id"]
    for name, value in uri_params["vars"].items():
        if name != "at" and name != "test":
            var = name + "=" + str(value)
            chords_uri = chords_uri + "&" + var

    if "at" in uri_params["vars"]:
        time_value = uri_params["vars"]["at"]
        # See if this is a unix time or an ISO time string
        if type(time_value) == int:
            ut = time.gmtime(time_value)
            timetag = "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z".format(
                ut[0], ut[1], ut[2], ut[3], ut[4], ut[5])
        else:
            timetag = time_value
        chords_uri = chords_uri + "&at=" + timetag

    if ("api_email" in uri_params) and ("api_key" in uri_params):
        if (uri_params["api_email"] != "") and (uri_params["api_key"] != ""):
            chords_uri = chords_uri + "&" + "email=" + str(uri_params["api_email"])
            chords_uri = chords_uri + "&" + "api_key=" + str(uri_params["api_key"])
    else:
        if "skey" in uri_params:
            if uri_params["skey"] != "":
                chords_uri = chords_uri + "&" + "key=" + str(uri_params["skey"])

    if "test" in uri_params:
        if uri_params["test"]:
            chords_uri = chords_uri + "&" + "test"

    return chords_uri

def submitURI(chords_uri, max_queue):
    """
    Put the uri on the send queue.
    """

    uri_queue_lock.acquire()
    if len(uri_queue) > max_queue:
        print("*** uri_queue full, ignoring message")
    else:
        uri_queue.append(chords_uri)
    uri_queue_lock.release()

def waiting():
    """
    Return the current length of the queue
    """
    n = 0
    uri_queue_lock.acquire()
    n = len(uri_queue)
    uri_queue_lock.release()
    return n

if __name__ == '__main__':

    chords_json = '{\
        "inst_id": "1",\
        "skey": "123456",\
        "vars": {\
        "at": 1511459453,\
        "lcount": 0,\
        "ldist": 0,\
        "pres": 769.2000000000001,\
        "rh": 30,\
        "tdry": 13.91,\
        "vbat": 3.47\
        }\
    }'

    # Start the sender thread.
    startSender()

    if len(sys.argv) != 2:
        print ("Usage:", sys.argv[0], "config_file")
        sys.exit(1)

    config = json.loads(open(sys.argv[1]).read())
    host = config["chords_host"]

    chords_stuff = json.loads(chords_json)
    chords_stuff['skey'] = config['skey']

    print (chords_stuff)
    for i in range(0, 10):
        uri = buildURI(host, chords_stuff)
        submitURI(uri, 20)
        time.sleep(1)

    while True:
        t = time.localtime()
        timestamp = "{:04}-{:02}-{:02} {:02}:{:02}:{:02}".format(t[0], t[1], t[2], t[3], t[4], t[5])
        print (timestamp, "Queue length: {:05}".format(waiting()))
        time.sleep(1)
