import os
import json
import requests
import pandas as pd

from dotenv import load_dotenv
from resources.utils import get_project_root

num_nodes_request = 10

load_dotenv()

URL = 'https://api.github.com/graphql'
TOKEN_LIST = (os.getenv("GITHUB_ACCESS_TOKENS"))
ROOT = get_project_root()


def load_json(file_dir):
	try:
		with open(file_dir, 'r') as read_file:
			return json.load(read_file)

	except FileNotFoundError:
		print(f'Failed to read data... Perform get_repos and assure data.json is in folder.')


def generate_new_header():
	new_header = {
		'Content-Type': 'application/json',
		'Authorization': f'bearer {os.getenv("GITHUB_ACCESS_TOKEN")}'
	}
	return new_header


# Query to be made on the current test
def create_query():
	query = """
	query github {
		search (query: "location:%s",  type: USER, first: 1) {
			userCount
		}
	}
	""" % location
	return query


def save_data(dataframe):
	dataframe.to_csv(f"{ROOT}/data_files_test/users_count.csv", index=False, header=True)


if __name__ == "__main__":
	print(f"\n**** Starting GitHub API Requests *****\n")
	locations_dir = f'{ROOT}/resources/refined_locations.json'
	locations = load_json(locations_dir)
	countries = locations.keys()
	token_index = 0
	headers = generate_new_header()

	df = pd.read_csv(f"{ROOT}/data_files_test/users_count.csv")
	for country in countries:
		print(f"\n**** Starting Country {country.upper()} Requests *****\n")
		total_country = 0
		for location in locations[country]:
			print(f"\n**** Starting Location {location} Requests *****\n")
			try:
				response = requests.post(f'{URL}', json={'query': create_query()}, headers=headers)
				response.raise_for_status()
				user_data = dict()
				data = dict(response.json())
				user_count = data['data']['search']['userCount']
				user_data['location'] = location
				user_data['count'] = user_count
				total_country += user_count
				df = df.append(user_data, ignore_index=True)
			except requests.exceptions.HTTPError:
				print('Erro de conexão...')

		df = df.append({'location': f'{country}_total_users_count', 'count': f'{total_country}'}, True)
		df = df.append({'location': f'{country}_target', 'count': f'{round(total_country * 0.1 + 0.5)}'}, True)

	save_data(df)


