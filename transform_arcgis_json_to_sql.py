## Migrating Research Data Collections Project
## Script to transform ArcGIS database details in JSON into SQL statements
## Script Author(s): Sam Sciolla
## Grant PI: Andrea Thomer

## Notes:
# - If the database you are working with has many-to-many relationships, the script may need to be re-evaluated to ensure proper results.
# - One-to-one relationships are handled by making foriegn-key fields UNIQUE.

import json
import requests

## Global variables
CACHE_FILE_NAME = 'arcgis_cache.json'
PROTECTED_FIELD_NAMES = ['condition']

# Setting up caching dictionary.
try:
    file_open = open(CACHE_FILE_NAME, "r")
    json_string = file_open.read()
    CACHE_DICTION = json.loads(json_string)
    file_open.close()
except:
    CACHE_DICTION = {}

## Functions

# Caching functions

# Create unique request string to identify request in cache
def make_unique_request_string(base_url, parameters_diction, private_keys=['api-key']):
    sorted_parameters = sorted(parameters_diction.keys())
    fields = []
    for parameter in sorted_parameters:
        if parameter not in private_keys:
            fields.append("{}-{}".format(parameter, parameters_diction[parameter]))
    return base_url + "&".join(fields)

# Pull data from API or from cache file
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

# Relationship analysis functions

# Create a dictionary with information about relationships
def parse_relationships(tables):
    relationship_matches = {}
    for table in tables:
        relationships = table['relationships']
        for relationship in relationships:
            relat_id = relationship['id']
            if relat_id not in relationship_matches:
                relationship_matches[relat_id] = {}
            if 'Origin' in relationship['role']:
                relationship_matches[relat_id]['Origin'] = relationship
                origin_table_name = table['name'].replace(' ', '_')
                if table['type'] in ['Feature Layer']:
                    origin_table_name += '_Layer'
                relationship_matches[relat_id]['Origin Table Name'] = origin_table_name
            elif 'Destination' in relationship['role']:
                relationship_matches[relat_id]['Destination'] = relationship
                destination_table_name = table['name'].replace(' ', '_')
                if table['type'] in ['Feature Layer']:
                    destination_table_name += '_Layer'
                relationship_matches[relat_id]['Destination Table Name'] = destination_table_name
            else:
                print("** Something's not working!! **")
    return relationship_matches

# Determine the order of the tables in the SQL script based on their foreign key indexes
def determine_table_order(relationship_matches_dict, table_name_strings):
    table_order_dict = {}
    for table_name in table_name_strings:
        table_order_dict[table_name] = {}
        table_order_dict[table_name]['Referenced Tables'] = []
    for key in relationship_matches_dict.keys():
        relationship_match = relationship_matches_dict[key]
        dest_table_name = relationship_match['Destination Table Name']
        origin_table_name = relationship_match['Origin Table Name']
        table_order_dict[dest_table_name]['Referenced Tables'].append(origin_table_name)

    # Order tables
    next_index = 0
    tables_ordered = []
    tables_still_to_order = True
    term_tables_still_to_order = True

    while tables_still_to_order:
        staging_area = []
        if term_tables_still_to_order:
            term_tables = []
            for table_name in table_order_dict.keys():
                referenced_tables = table_order_dict[table_name]['Referenced Tables']
                if len(referenced_tables) == 0:
                    term_tables.append(table_name)
            staging_area += term_tables
            term_tables_still_to_order = False
        else:
            for table_name in table_order_dict.keys():
                if table_name not in tables_ordered:
                    referenced_tables = table_order_dict[table_name]['Referenced Tables']
                    ref_table_num = 1
                    for referenced_table in referenced_tables:
                        if referenced_table in tables_ordered:
                            if ref_table_num == len(referenced_tables):
                                staging_area.append(table_name)
                            ref_table_num += 1
                        else:
                            break
        # Assign order indexes to tables in staging area
        staging_area = sorted(staging_area)
        for table_name in staging_area:
            table_order_dict[table_name]['Order Index'] = next_index
            next_index += 1
            tables_ordered.append(table_name)
        if len(tables_ordered) == len(table_order_dict.keys()):
            tables_still_to_order = False

    return table_order_dict

# Generate a SQL CREATE statement as a single multi-line string
def write_create_table_statement(table_dict, new_table_name, relationship_matches):
    # Determine foreign key lines and store index info for later
    local_origin_keys = []
    unique_destination_keys = []
    foreign_key_lines = []
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
        # If field name is known to be protected, add an underscore to the end of the field name
        if field_name.lower() in PROTECTED_FIELD_NAMES:
            field_name += '_'
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
            print("** GUID data type found! {} {} **".format(new_table_name, field['name']))
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

    # Write primary key line
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
COLLATE utf8mb4_0900_ai_ci;'''.format(new_table_name, main_block)
    return create_statement

# Create string with SQL preamble containing a list of tables to drop and the name of the database to use
def create_preamble(table_list, database_name):
    tables_string = ", ".join(table_list)
    preamble = '''USE {};
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS {};
SET FOREIGN_KEY_CHECKS=1;
'''.format(database_name, tables_string)
    return preamble

# Run the transformation workflow on a ArcGIS database specified by URL
def run_transformation(database_base_url, database_name):
    # Pulling data from ArcGIS Server
    parameters = {'f':'json'}
    data = fetch_API_data(database_base_url, parameters)
    layers = data['layers']
    tables = data['tables']
    all_tables = layers + tables
    arcgis_json_file = open('arcgis_response.json', 'w', encoding='utf-8')
    arcgis_json_file.write(json.dumps(data, indent=4))
    arcgis_json_file.close()

    # Identify pairs of tables connected through relationships
    relationship_matches = parse_relationships(all_tables)
    relationship_matches_file = open('relationship_matches.json', 'w', encoding='utf-8')
    relationship_matches_file.write(json.dumps(relationship_matches, indent=4))
    relationship_matches_file.close()

    # Produce SQL script statments
    sql_statements = {}
    for table in all_tables:
        table_name = table['name'].replace(' ', '_')
        if 'Layer' in table['type']:
            table_name += '_Layer'
        print('// {} //'.format(table_name))
        table_statement = write_create_table_statement(table, table_name, relationship_matches)
        sql_statements[table_name] = table_statement
    all_table_names = sql_statements.keys()

    # Write SQL script
    ordering_result = determine_table_order(relationship_matches, all_table_names)

    all_tables_in_order = sorted(sql_statements.keys(), key=lambda x: ordering_result[x]['Order Index'])
    sql_text = create_preamble(all_table_names, database_name)
    for next_table_name in all_tables_in_order:
        sql_text += '\n' + sql_statements[next_table_name] + '\n'
    tables_sql_file = open('database_tables.sql', 'w')
    tables_sql_file.write(sql_text)
    tables_sql_file.close()

## Main Program

if __name__ == '__main__':
    print('** ArcGIS JSON to MySQL Transformation Script **')
    arcgis_database_base_url = 'https://services1.arcgis.com/4ezfu5dIwH83BUNL/arcgis/rest/services/MBGNA_Database_Local/FeatureServer//layers'
    new_database_name = 'MBGNA_ArcGIS'
    run_transformation(arcgis_database_base_url, new_database_name)
