## Script to transform ArcGIS JSON output into SQL statements for database
## Migrating Research Data Collections Project
## Grant PI: Andrea Thomer
## Script Author: Sam Sciolla

## Notes:
# - The order of tables in the output SQL files needs be examined and potentially fixed to ensure tables refererenced by foreign keys are created first. (I have plans to create an algorithm to fix this soon.)
# - If the database you are working with has many-to-many relationships, the script may need to be re-evaluated to ensure proper results.
# - One-to-one relationships are handled by making foriegn-key fields UNIQUE.

import json
import requests

# Global variables
CACHE_FILE_NAME = 'mbgna_arcgis_cache.json'

## Caching

# Setting up caching dictionary.
try:
    file_open = open(CACHE_FILE_NAME, "r")
    json_string = file_open.read()
    CACHE_DICTION = json.loads(json_string)
    file_open.close()
except:
    CACHE_DICTION = {}

def make_unique_request_string(base_url, parameters_diction, private_keys=['api-key']):
    sorted_parameters = sorted(parameters_diction.keys())
    fields = []
    for parameter in sorted_parameters:
        if parameter not in private_keys:
            fields.append("{}-{}".format(parameter, parameters_diction[parameter]))
    return base_url + "&".join(fields)

def fetch_API_data(base_url, parameters_diction):
    unique_request_url = make_unique_request_string(base_url, parameters_diction)
    if unique_request_url in CACHE_DICTION:
        print("** Pulling data from cache **")
        return CACHE_DICTION[unique_request_url]
    else:
        print("** Fetching new data from API **")
        response = requests.get(base_url, parameters_diction)
        data = json.loads(response.text)
        CACHE_DICTION[unique_request_url] = data
        cache_file_open = open(CACHE_FILE_NAME, "w")
        cache_file_open.write(json.dumps(CACHE_DICTION))
        cache_file_open.close()
        return data

## Data manipulation

def extract_info_from_table(entity_dict):
    table = {}
    table['name'] = entity_dict['name']
    table['type'] = entity_dict['type']
    fields = []
    for field_dict in entity_dict['fields']:
        field = {}
        field['name'] = field_dict['name']
        field['type'] = field_dict['type']
        if 'length' in field_dict.keys():
            field['length'] = field_dict['length']
        field['nullable'] = field_dict['nullable']
        field['domain'] = field_dict['domain']
        if field['domain'] != None:
            print("*** Domain found! ***")
            print(table['name'])
            print(field['name'])
        fields.append(field)
    table['fields'] = fields
    table['relationships'] = entity_dict['relationships']
    table['indexes'] = entity_dict['indexes']

    return table

def parse_relationships(tables):
    relationship_matches = {}
    for table in tables:
        relationships = table['relationships']
        for relationship in relationships:
            relat_id = relationship['id']
            if relat_id not in relationship_matches:
                relationship_matches[relat_id] = {}
            if 'Origin' in relationship['role'] and 'Origin' not in relationship_matches[relat_id]:
                relationship_matches[relat_id]['Origin'] = relationship
                origin_table_name = table['name'].replace(' ', '_')
                if table['type'] in ['Feature Layer']:
                    origin_table_name += '_Layer'
                relationship_matches[relat_id]['Origin Table Name'] = origin_table_name
            elif 'Destination' in relationship['role'] and 'Destination' not in relationship_matches[relat_id]:
                relationship_matches[relat_id]['Destination'] = relationship
                destination_table_name = table['name'].replace(' ', '_')
                if table['type'] in ['Feature Layer']:
                    destination_table_name += '_Layer'
                relationship_matches[relat_id]['Destination Table Name'] = destination_table_name
            else:
                print("** Something's not working!! **")
    return relationship_matches

def write_create_table_statement(table_dict, relationship_matches):

    # Name table
    table_name = table_dict['name'].replace(' ', '_')
    if 'Layer' in table_dict['type']:
        table_name += '_Layer'

    # Determine foreign key lines and store index info for later
    local_origin_keys = []
    unique_destination_keys = []
    foreign_key_lines = []
    # print(table_dict['relationships'])
    for relationship in table_dict['relationships']:
        relat_id = relationship['id']
        relationship_match = relationship_matches[relat_id]
        if 'Destination' in relationship['role']:
            destination_key = relationship_match['Destination']['keyField']
            if 'OneToOne' in relationship_match['Origin']['cardinality']:
                unique_destination_key = destination_key
                unique_destination_keys.append(unique_destination_key)
            origin_key = relationship_match['Origin']['keyField']
            origin_table_name = relationship_match['Origin Table Name']
            foreign_key_line = '    FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE CASCADE ON UPDATE CASCADE'.format(destination_key, origin_table_name, origin_key)
            foreign_key_lines.append(foreign_key_line)
        elif 'Origin' in relationship['role']:
            local_origin_key = relationship_match['Origin']['keyField']
            local_origin_keys.append(local_origin_key)

    # Create attribute definition chunk
    attribute_lines = []
    for field in table_dict['fields']:
        extras = None
        data_type = None
        field_name = field['name']

        if 'String' in field['type']:
            length = field['length']
            data_type = 'VARCHAR({})'.format(str(length))
        elif 'Integer' in field['type']:
            data_type = 'INTEGER'
        elif 'Double' in field['type']:
            data_type = 'DOUBLE'
        elif 'Date' in field['type']:
            data_type = 'DATE'
        elif 'GUID' in field['type']:
            print("** GUID data type found! {} {} **".format(table_name, field['name']))
            data_type = 'INTEGER'
        elif 'OID' in field['type']:
            data_type = 'INTEGER'
            id_attribute_name = field['name']
            extras = 'AUTO_INCREMENT UNIQUE'
        else:
            print("** Some other data type is present! **")
            print(field['type'])

        if field['nullable'] == True:
            nullable = 'NULL'
        else:
            nullable = 'NOT NULL'

        if field_name in local_origin_keys:
            extras = 'UNIQUE'

        if field_name in unique_destination_keys:
            extras = 'UNIQUE'

        attribute_line = '    ' + ' '.join([field_name, data_type, nullable])
        if extras != None:
            attribute_line += ' ' + extras
        attribute_lines.append(attribute_line)

    # Primary key line
    primary_key_line = '    PRIMARY KEY ({})'.format(id_attribute_name)

    # Put everything together
    main_block = ',\n'.join(attribute_lines + [primary_key_line] + foreign_key_lines)

    create_statement = '''
CREATE TABLE IF NOT EXISTS {}
  (
{}
  )
ENGINE=InnoDB
CHARACTER SET utf8mb4
COLLATE utf8mb4_0900_ai_ci;'''.format(table_name, main_block)
    return create_statement

def create_preamble(table_list):
    tables_string = ", ".join(table_list)
    preamble = '''
USE MBGNA_ArcGIS;

SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS {};
SET FOREIGN_KEY_CHECKS=1;
               '''.format(tables_string)
    return preamble

## Main Program

print('-- ArcGIS JSON to MySQL Transformation Script --')

# Pulling data from ArcGIS Server

base_url = 'https://services1.arcgis.com/4ezfu5dIwH83BUNL/arcgis/rest/services/MBGNA_Database_Local/FeatureServer//layers'
parameters = {'f':'json'}
data = fetch_API_data(base_url, parameters)

arcgis_json_file = open('arcgis_json_file.json', 'w', encoding='utf-8')
arcgis_json_file.write(json.dumps(data, indent=4))
arcgis_json_file.close()

layers = data['layers']
tables = data['tables']
all_tables = layers + tables

# Identifying tables connected through relationships

relationship_matches = parse_relationships(all_tables)

relationship_matches_file = open('relationship_matches.json', 'w', encoding='utf-8')
relationship_matches_file.write(json.dumps(relationship_matches, indent=4))
relationship_matches_file.close()

# Creating SQL scripts

table_sql_statements = []
table_names = []
for table in tables:
    print('// {} //'.format(table['name']))
    table_names.append(table['name'].replace(' ', '_'))
    table_statement = write_create_table_statement(table, relationship_matches)
    table_sql_statements.append(table_statement)
table_sql_statements = sorted(table_sql_statements, key=lambda x: x.count('FOREIGN KEY'))

tables_sql_file = open('mbgna_arcgis_one.sql', 'w')
tables_sql_text = create_preamble(table_names) + '\n'.join(table_sql_statements)
tables_sql_file.write(tables_sql_text)
tables_sql_file.close()

layer_sql_statements = []
layer_names = []
for layer in layers:
    print('// {} //'.format(layer['name']))
    layer_names.append(layer['name'].replace(' ', '_') + '_Layer')
    layer_statement = write_create_table_statement(layer, relationship_matches)
    layer_sql_statements.append(layer_statement)
layer_sql_statements = sorted(layer_sql_statements, key=lambda x: x.count('FOREIGN KEY'))

layers_sql_file = open('mbgna_arcgis_two.sql', 'w')
layers_sql_text = create_preamble(layer_names) + '\n'.join(layer_sql_statements)
layers_sql_file.write(layers_sql_text)
layers_sql_file.close()
