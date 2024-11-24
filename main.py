import json
import requests

# Solr connection URL (update based on your local Solr instance)
SOLR_URL = 'http://localhost:8989/solr/'

# Collection names
collections = ['Hash_Soundarrajan', 'Hash_9708']

# Sample employee data (this should be from your dataset)
data = [
    {
        "id": "1",
        "Employee_ID": "E02002",
        "Full_Name": "Kai Le",
        "Job_Title": "Controls Engineer",
        "Department": "Manufacturing",
        "Business_Unit": "Manufacturing",
        "Gender": "Male",
        "Ethnicity": "Asian",
        "Age": 47,
        "Hire_Date": "2/5/2022",
        "Annual_Salary": "$92,368",
        "Bonus__": "0%",
        "Country": "United States",
        "City": "Columbus",
        "Exit_Date": 0
    },
    # Add more employee records here if necessary
]

def check_collection_exists(collection_name):
    """Check if the collection already exists in Solr"""
    response = requests.get(f"{SOLR_URL}{collection_name}/schema")
    if response.status_code == 200:
        print(f"Collection {collection_name} exists.")
        return True
    else:
        print(f"Collection {collection_name} does not exist.")
        return False

def update_solr(collection_name, data):
    """Update Solr with employee data"""
    try:
        # Send data in JSON format
        headers = {'Content-Type': 'application/json'}
        response = requests.post(f"{SOLR_URL}{collection_name}/update?commit=true", 
                                 headers=headers, data=json.dumps(data))
        
        if response.status_code == 200:
            print(f"Data successfully updated to {collection_name}.")
        else:
            print(f"Error updating Solr collection {collection_name}: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")

def delete_employee_from_collection(collection_name, employee_id):
    """Delete an employee from the collection"""
    try:
        # Send a delete request for the employee
        headers = {'Content-Type': 'application/json'}
        delete_data = [{"delete": {"id": employee_id}}]
        response = requests.post(f"{SOLR_URL}{collection_name}/update?commit=true", 
                                 headers=headers, data=json.dumps(delete_data))
        
        if response.status_code == 200:
            print(f"Employee {employee_id} successfully deleted from {collection_name}.")
        else:
            print(f"Error deleting employee {employee_id} from {collection_name}: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")

def search_in_collection(collection_name, field, value):
    """Search for employees in a collection based on a field and value"""
    try:
        query = f"{SOLR_URL}{collection_name}/select?q={field}:{value}&wt=json"
        response = requests.get(query)
        
        if response.status_code == 200:
            result = response.json()
            if result['response']['numFound'] > 0:
                print(f"Found {result['response']['numFound']} employees matching {field}={value}.")
                for doc in result['response']['docs']:
                    print(doc)
            else:
                print(f"No employees found with {field}={value}.")
        else:
            print(f"Error searching in {collection_name}: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")

def get_column_facets(collection_name, field):
    """Get facets for a specific column in a collection"""
    try:
        query = f"{SOLR_URL}{collection_name}/select?q=:&facet=true&facet.field={field}&wt=json"
        response = requests.get(query)
        
        if response.status_code == 200:
            result = response.json()
            if 'facet_counts' in result:
                print(f"Facets for {field} in {collection_name}:")
                print(result['facet_counts']['facet_fields'][field])
            else:
                print(f"No facets found for {field}.")
        else:
            print(f"Error getting facets in {collection_name}: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Solr: {e}")

def main():
    for collection in collections:
        # Check if the collection exists
        if not check_collection_exists(collection):
            print(f"Skipping {collection} due to non-existence.")
            continue

        # Update data in Solr
        update_solr(collection, data)

        # Search and fetch by specific column (e.g., Department, Gender)
        search_in_collection(collection, "Department", "IT")
        search_in_collection(collection, "Gender", "Male")

        # Get column facets (e.g., Department)
        get_column_facets(collection, "Department")

        # Example of deleting an employee
        delete_employee_from_collection(collection, "E02003")

if _name_ == '_main_':
    main()