"""
@author: MANOJ KUMAR B G 

"""

# Import necessary modules
import concurrent.futures
from bs4 import BeautifulSoup
import requests
import json

# Define a function to extract job details from a given job id and return them as a dictionary
def get_job_details(id, HEADERS):
    # Append the id to the URL to get the job posting page
    url = f"https://www.smartrecruiters.com/Cermaticom/{id}"
    new_webpage = requests.get(url, headers=HEADERS)
    new_soup = BeautifulSoup(new_webpage.content, "html.parser")
    
    # Extract the job location from the page
    try:
        location = new_soup.find('span', {'itemprop': 'address'}).find('spl-job-location')['formattedaddress']
    except AttributeError:
        location = ""
    
    # Extract the job qualifications from the page
    try:
        job_qualifications = [li.text.strip() for li in new_soup.find('div', {'itemprop': 'qualifications'}).find_all('li')]
        if job_qualifications == []:
            job_qualifications = [new_soup.find('div', {'itemprop': 'qualifications'}).text.strip()]
    except AttributeError:
        try:
            job_qualifications = [li.text.strip() for li in new_soup.find('div', {'itemprop':'incentives',}).find_all('li')]
        except AttributeError:
            job_qualifications =[]
    
    # Extract the job description from the page
    try:
        job_descr = [li.text.strip() for li in new_soup.find('div', {'itemprop': 'responsibilities'}).find_all('li')]
        if job_descr == []:
            job_descr =  [new_soup.find('div', {'itemprop': 'description'}).text.strip()] 
    except AttributeError:
            job_descr =[]
    
    # Return the job details as a dictionary
    return {'location': location, 'job_qualifications': job_qualifications, 'job_descr': job_descr}

# Define a function to scrape job data for a given item and return it as a tuple
def scrape_job(country, item, HEADERS):
    # Extract job id, name, department, and creator from the item
    id = item['id']
    name = item.get('name', '')
    department_name = item.get('department', {}).get('label', '')
    creator_name = item.get('creator', {}).get('name', '')
    
    # Get the job details for the job id
    job_details = get_job_details(id, HEADERS)
    
    # Return a tuple containing the department name and job data as a dictionary
    return (department_name, {'title': name, 'location': job_details['location'], 'description': job_details['job_descr'], 'qualification': job_details['job_qualifications'], 'posted by': creator_name})

# Define main function
if __name__ == '__main__':
    # Specify the URL and headers to access the webpage
    URL = 'https://www.cermati.com/karir'
    HEADERS = {'User-Agent': '', 'Accept-Language': 'en-US, en;q=0.5'}
    
    # Get the webpage and parse it with BeautifulSoup
    webpage = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(webpage.content, "html.parser")
    
    # Extract the job posting data from the JavaScript object in the HTML
    links = soup.find_all("script", attrs={'id': 'initials'})
    json_str = links[0].string.strip()
    
    # Parse the JSON string into a Python dictionary
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        print("Error parsing JSON string.")
        
    # Define the list of countries to scrape
    country_list = ['india', 'indonesia', 'singapore']
    
    # Define an empty dictionary to hold the scraped job posting data
    my_dict = {}
    
    # Use a ProcessPoolExecutor to scrape job posting data concurrently
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        
        # Loop over each country and job posting, submitting each job to the executor
        for country in country_list:
            for item in data['smartRecruiterResult'][country]['content']:
                futures.append(executor.submit(scrape_job, country, item, HEADERS))
        
        # Wait for all of the jobs to complete, collecting the results as they finish
        for future in concurrent.futures.as_completed(futures):
            department_name, job_data = future.result()
            
            # Add the job data to the dictionary, grouped by department name
            my_dict.setdefault(department_name, []).append(job_data)
   
    # Reformat the dictionary to match the desired output format
    output_dict = {}
    for department_name, jobs in my_dict.items():
        output_dict[department_name] = jobs
    
    # Save the output to a JSON file and print a message indicating completion
    with open('solution.json', 'w') as f:
         json.dump(output_dict, f, indent=4)
         print("done")

    
