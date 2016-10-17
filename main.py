import xml.etree.ElementTree as xml
import psycopg2
import re
import math

tree = xml.parse("C:\Users\Ankur Bansal\Desktop\singapore.osm")
root = tree.getroot()
nodes = {}
osmIds = {}
osm = 1
nodeId = 1000
def longtoInt(node):
    global nodeId, nodes
    if node in nodes:
        return nodes[node]
    else:
        nodeId += 1
        nodes[node] = nodeId
        return nodeId

def mapOsmId(osm_id):
    global osmIds,osm
    if osm_id in osmIds:
        return osmIds[osm]
    else:
        osm+=1
        osmIds[osm_id] = osm
        return osm


def dbConnect():
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    print "Connection Successful"
    return conn


def parser():
    nodes = {}
    ways = {}
    query = """INSERT INTO "public.osm_data"
    (id, osm_id, osm_name, osm_source_id, osm_target_id, clazz, source, target, km, kmh, x1, y1, x2, y2)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    conn = dbConnect()
    cur = conn.cursor()
    for elem in root:
        if elem.tag == 'node':
            nodes[long(elem.attrib['id'])] = {"lat" : elem.attrib['lat'], "lon" : elem.attrib['lon']}

        elif elem.tag == 'way':
            wayId = elem.attrib['id']
            wayNodes = []
            tags = {}
            for child in elem:
                if child.tag == 'nd':
                    wayNodes.append(long(child.attrib['ref']))
                elif child.tag == 'tag':
                    tags[child.attrib['k']] = child.attrib['v']
            ways[long(wayId)] = {"nodes" : wayNodes, "tags" : tags}

    for key,way in ways.iteritems():
        clazz = way['tags'].get('highway')
        if clazz:
            osm_id = key
            id = mapOsmId(osm_id)
            osm_name = way['tags'].get('name')
            if(type(osm_name) == unicode):
                osm_name = osm_name.encode("utf-8")

            for i in range(len(way['nodes']) - 1):
                osm_source_id = way['nodes'][i]
                osm_target_id = way['nodes'][i+1]
                source = int(longtoInt(osm_source_id))
                target = int(longtoInt(osm_target_id))
                kmh = int(re.findall('\d+', way['tags'].get('maxspeed', "40"))[0])
                x1 = float(nodes[osm_source_id]['lat'])
                y1 = float(nodes[osm_source_id]['lon'])
                x2 = float(nodes[osm_target_id]['lat'])
                y2 = float(nodes[osm_target_id]['lon'])
                km = math.sqrt(math.pow((x1 - x2), 2) + math.pow((y1 - y2), 2))
                data = (
                id, osm_id, osm_name, osm_source_id, osm_target_id, clazz, source, target, km, kmh, x1, y1, x2, y2)
                cur.execute(query, data)
                if not (way['tags'].get('oneway') == "yes"):
                    data = (
                    id, osm_id, osm_name, osm_target_id, osm_source_id, clazz, source, target, km, kmh, x2, y2, x1, y1)
                    cur.execute(query, data)
            # osm_source_id = way['nodes'][0]
            # osm_target_id = way['nodes'][-1]


    conn.commit()
    conn.close()

parser()