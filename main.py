import xml.etree.ElementTree as xml
import psycopg2
import re
tree = xml.parse("C:\Users\Ankur Bansal\Desktop\map.osm")
root = tree.getroot()

def dbConnect():
    conn = psycopg2.connect(database="postgres", user="postgres", password="password", host="127.0.0.1", port="5432")
    return conn


def parser():
    nodes = {}
    ways = {}
    query = """INSERT INTO "public.osm_data" (id,osm_id,osm_name,osm_source_id,osm_target_id,kmh,x1,y1,x2,y2) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
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

    id = 0
    for key,way in ways.iteritems():
        clazz = way['tags'].get('highway')
        if clazz:
            id +=1
            osm_id = key
            osm_name = way['tags'].get('name')
            osm_source_id = way['nodes'][0]
            osm_target_id = way['nodes'][-1]
            kmh = int(re.findall('\d+',way['tags'].get('maxspeed',"40"))[0])
            x1 = nodes[osm_source_id]['lat']
            y1 = nodes[osm_source_id]['lon']
            x2 = nodes[osm_target_id]['lat']
            y2 = nodes[osm_target_id]['lon']
            if way['tags'].get('oneway') == "yes":
                data = (id,osm_id,osm_name,osm_source_id,osm_target_id,kmh,x1,y1,x2,y2)
                cur.execute(query,data)
                conn.commit
            else:
                data = (id, osm_id, osm_name, osm_source_id, osm_target_id, kmh, x1, y1, x2, y2)
                cur.execute(query, data)
                conn.commit
                id +=1
                data = (id, osm_id, osm_name, osm_target_id, osm_source_id, kmh, x2, y2, x1, y1)
                cur.execute(query, data)
                conn.commit

    conn.close()

parser()