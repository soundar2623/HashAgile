import requests
import pandas as pd
import json

SOLR_URL = "http://localhost:8989/solr"

def make_request(url, method="GET", headers=None, data=None):
    """Helper function to handle HTTP requests to Solr."""
    try:
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, data=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        # Check if response is successful
        response.raise_for_status()  # Raises an exception for HTTP errors (400-500)
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")
        return None

def create_collection(collection_name):
    """Create a collection if it doesn't exist."""
    url = f"{SOLR_URL}/admin/collections?action=LIST"
    response = make_request(url)
    
    if response and 'collections' in response:
        collections = response['collections']
        if collection_name in collections:
            print(f"Collection {collection_name} already exists.")
        else:
            url = f"{SOLR_URL}/admin/collections?action=CREATE&name={collection_name}&numShards=1&replicationFactor=1"
            response = make_request(url)
            print(f"Create Collection {collection_name}: {response}")
    else:
        print(f"Error fetching collection list or Solr response: {response}")
    
    return response

def index_data(collection_name, file_path, exclude_column=None):
    """Index data from a CSV file into Solr."""
    data = pd.read_csv(file_path, encoding='ISO-8859-1')
    print(f"Columns in the dataset: {data.columns.tolist()}")
    
    # Drop the excluded column, if any
    if exclude_column and exclude_column in data.columns:
        data = data.drop(columns=[exclude_column])
    
    # Format 'Exit Date' if it exists
    if 'Exit Date' in data.columns:
        data['Exit Date'] = pd.to_datetime(data['Exit Date'], errors='coerce')
        data['Exit Date'] = data['Exit Date'].dt.strftime('%Y-%m-%d')
        data['Exit Date'] = data['Exit Date'].fillna('null')
    else:
        print("'Exit Date' column is missing, skipping date formatting.")
    
    # Convert the DataFrame to a list of dictionaries
    json_data = data.to_dict(orient="records")
    
    # Prepare headers and the Solr URL for update
    headers = {"Content-Type": "application/json"}
    url = f"{SOLR_URL}/{collection_name}/update?commit=true"
    
    try:
        # Send the data to Solr
        response = requests.post(url, headers=headers, data=json.dumps(json_data))
        response.raise_for_status()  # Will raise an HTTPError for bad responses
        print(f"Index Data in {collection_name}: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")
    
    return response.json()

def search_by_column(collection_name, column_name, column_value):
    """Search for records by a specific column value."""
    query = f"{column_name}:{column_value}"
    url = f"{SOLR_URL}/{collection_name}/select?q={query}"
    response = make_request(url)
    if response:
        print(f"Search by Column {column_name}={column_value} in {collection_name}: {response}")
    return response

def get_emp_count(collection_name):
    """Get the total employee count in a collection."""
    url = f"{SOLR_URL}/{collection_name}/select?q=:&rows=0"
    response = make_request(url)
    if response:
        count = response['response']['numFound']
        print(f"Employee Count in {collection_name}: {count}")
    return count

def del_emp_by_id(collection_name, employee_id):
    """Delete an employee by ID."""
    url = f"{SOLR_URL}/{collection_name}/update?commit=true"
    delete_query = {"delete": {"id": employee_id}}
    headers = {"Content-Type": "application/json"}
    response = make_request(url, method="POST", headers=headers, data=json.dumps(delete_query))
    if response:
        print(f"Delete Employee ID {employee_id} in {collection_name}: {response}")
    return response

def get_dep_facet(collection_name):
    """Get facet data for the 'Department' field."""
    url = f"{SOLR_URL}/{collection_name}/select?q=:&facet=true&facet.field=Department&rows=0"
    response = make_request(url)
    if response and 'facet_counts' in response:
        facets = response['facet_counts']['facet_fields']['Department']
        print(f"Department Facets in {collection_name}: {facets}")
        return facets
    else:
        print(f"No facets returned for {collection_name} or error occurred.")
        return []

if _name_ == "_main_":
    # File path to the employee dataset CSV file
    dataset_path = "Employee_Sample_Data_1.csv"  # Update with the correct path
    name_collection = "Hash_Soundarrajan"
    phone_collection = "Hash_9708"
    
    # Create collections if they don't exist
    create_collection(name_collection)
    create_collection(phone_collection)
    
    # Index data from CSV into collections
    index_data(name_collection, dataset_path, exclude_column="Department")
    index_data(phone_collection, dataset_path, exclude_column="Gender")
    
    # Get the employee count in each collection
    get_emp_count(name_collection)
    get_emp_count(phone_collection)
    
    # Delete an employee by ID
    del_emp_by_id(name_collection, "E02003")
    get_emp_count(name_collection)
    
    # Search by columns in collections
    search_by_column(name_collection, "Department", "IT")
    search_by_column(name_collection, "Gender", "Male")
    search_by_column(phone_collection, "Department", "IT")
    
    # Get department facets in collections
    get_dep_facet(name_collection)
    get_dep_facet(phone_collection)