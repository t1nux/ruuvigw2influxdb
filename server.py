from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import json
import math
import struct
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

token = 'asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfas=='
org = 'organization'
bucket = 'bucket'
appID = 'asdfasdfasdf-asdfasdfasdfa'  # if you use the app
GWmac = 'C0:FF:EE:C0:FF:EE'  # if you use a gateway, MAC of the gateway
src = 'gw'  # 'gw' for gateway, 'app' for app
debug = False
port = 6464  # on which port to listen

# identify your tags
tagIDs = {'CA:B0:0D:1E:00:00': 'attic',
          'CA:B0:0D:1E:00:01': 'office',
          'CA:B0:0D:1E:00:02': 'garage',
          'CA:B0:0D:1E:00:03': 'outside',
          'CA:B0:0D:1E:00:04': 'bedroom',
          'CA:B0:0D:1E:00:05': 'living'}

client = InfluxDBClient(url='https://www.example.com', token=token)
writeapi = client.write_api(write_options=SYNCHRONOUS)


def eq_VP(temp):
    return 611.2 * math.exp(17.67 * temp / (243.5 + temp))


def dewpoint(temp, hum):
    tmp = math.log(hum / 100 * eq_VP(temp) / 611.2)
    return -243.5 * tmp / (tmp - 17.67)


def get_data_v5(s, name):
    """The data format was implemented according to https://github.com/ruuvi/ruuvi-sensor-protocols/blob/master/dataformat_05.md"""
    bb = bytes([int(s[2*i:2*(i+1)], 16) for i in range(len(s)//2)])
    fmt = '>BhHHhhhHBH'
    ver, temp, hum, pres, accx, accy, accz, pw_sig, movc, measID = struct.unpack_from(fmt, bb)
    temp *= 0.005
    hum = min(hum, 40000) * 0.0025
    pres = (pres + 50000)
    accx /= 1e3
    accy /= 1e3
    accz /= 1e3
    pw = (pw_sig >> 5) / 1000 + 1.6
    sig = (pw_sig & 0b11111) * 2 - 40
    dp = dewpoint(temp, hum)
    rdata = {'measurement':'env_data',
             'tags': {'location': name},
             'fields': {'temperature': temp,
                        'humidity': hum,
                        'dewpoint': dp,
                        'pressure': pres/100,  # kPa -> mbar
                        'movementCounter': movc,
                        'voltage': pw}}
    return rdata


def get_data_gw(post):
    iddata = json.loads(post)['data']['gw_mac']
    if iddata != GWmac:
        return
    data = json.loads(post)['data']['tags']
    for tag in data:
        if tag in tagIDs:
            dat = get_data_v5(data[tag]['data'][14:], tagIDs[tag])  # truncate manufacturer ID
            writeapi.write(bucket, org, dat)


def get_data_app(post):
    iddata = json.loads(post)['deviceId']
    if iddata != appID:
        return
    data = json.loads(post)['tags'][0]
    temp = float(data['temperature'])
    hum = float(data['humidity'])
    pres = float(data['pressure'])
    movc = int(data['movementCounter'])
    pw = float(data['voltage'])
    dp = dewpoint(temp, hum)
    rdata = {'measurement':'env_data',
             'tags': {'location': data['name']},
             'fields': {'temperature': temp,
                        'humidity': hum,
                        'dewpoint': dp,
                        'pressure': pres/100,  # kPa -> mbar
                        'movementCounter': movc,
                        'voltage': pw}}
    writeapi.write(bucket, org, rdata)


class Srv(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        logging.debug('GET request,\nPath: {}\nHeaders:\n{}\n'.format(str(self.path), str(self.headers)))
        self._set_response()
        self.wfile.write('GET request for {}'.format(self.path).encode('utf-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        logging.debug('POST request,\nPath: {}\nHeaders:\n{}\n\nBody:\n{}\n'.format(str(self.path), str(self.headers), post_data.decode('utf-8')))
        get_data_gw(post_data) if src == 'gw' else get_data_app(post_data)
        self._set_response()
        self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))


def run(server_class=HTTPServer, handler_class=Srv, port=port):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')


if __name__ == '__main__':
    run()
